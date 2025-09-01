import asyncio
import logging
import os
from datetime import datetime
import json
import xxhash
import copy
import re
import time
import threading
from functools import partial
from io import BytesIO
from multiprocessing.context import TimeoutError
from timeit import default_timer as timer
import tracemalloc
import signal
import random
import sys
from peewee import DoesNotExist

from app.utils.log_utils import get_project_base_directory, initRootLogger
from app.database.redis_database import REDIS_CONN, Payload
from app.database.services.task_service import TaskService
from app.database.services.file_service import FileService
from app.database.db_models import close_connection
from app.database.settings import SVR_QUEUE_NAME, FILE_MAXIMUM_SIZE
from app.database import TaskStatus, LLMType
from app.database.storage_factory import STORAGE_IMPL
from app.database.services.llm_service import LLMBundle
from app import settings

CONSUMER_NO = "0" if len(sys.argv) < 2 else sys.argv[1]
CONSUMER_NAME = "task_executor_" + CONSUMER_NO
initRootLogger(CONSUMER_NAME)

CONSUMER_NAME = "task_consumer_" + CONSUMER_NO
PAYLOAD: Payload | None = None
BOOT_AT = datetime.now().astimezone().isoformat(timespec="milliseconds")
PENDING_TASKS = 0
LAG_TASKS = 0

mt_lock = threading.Lock()
DONE_TASKS = 0
FAILED_TASKS = 0
CURRENT_TASK = None

tracemalloc_started = False

# SIGUSR1 handler: start tracemalloc and take snapshot
def start_tracemalloc_and_snapshot(signum, frame):
    global tracemalloc_started
    if not tracemalloc_started:
        print("got SIGUSR1, start tracemalloc")
        tracemalloc.start()
        tracemalloc_started = True
    else:
        print("got SIGUSR1, tracemalloc is already running")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_file = f"snapshot_{timestamp}.trace"
    snapshot_file = os.path.abspath(os.path.join(get_project_base_directory(), "logs", f"{os.getpid()}_snapshot_{timestamp}.trace"))

    snapshot = tracemalloc.take_snapshot()
    snapshot.dump(snapshot_file)
    print(f"taken snapshot {snapshot_file}")

# SIGUSR2 handler: stop tracemalloc
def stop_tracemalloc(signum, frame):
    global tracemalloc_started
    if tracemalloc_started:
        print("go SIGUSR2, stop tracemalloc")
        tracemalloc.stop()
        tracemalloc_started = False
    else:
        print("got SIGUSR2, tracemalloc not running")

class TaskCanceledException(Exception):
    def __init__(self, msg):
        self.msg = msg

def set_progress(task_id, prog=None, msg="Processing..."):
    """
    参数:
        task_id: 当前任务的唯一标识符。
        from_page: 起始页码（默认为0）。
        to_page: 结束页码（默认为-1）。
        prog: 进度值，可以为空或负数表示错误状态。
        msg: 进度消息内容。
    返回值: 无返回值，但会更新数据库中的任务进度信息，并在特定条件下抛出异常。
    功能: 更新指定任务的进度和状态，包括取消任务的处理逻辑。如果任务被取消，则调用PAYLOAD.ack()确认消息已消费，并引发TaskCanceledException异常。
    """
    global PAYLOAD
    if prog is not None and prog < 0:
        msg = "[ERROR]" + msg
    try:
        cancel = TaskService.do_cancel(task_id)
    except DoesNotExist:
        logging.warning(f"set_progress task {task_id} is unknown")
        if PAYLOAD:
            PAYLOAD.ack()
        return
    if cancel:
        msg += " [Canceled]"
        prog = -1

    if msg:
        msg = datetime.now().strftime("%H:%M:%S") + " " + msg
    d = {"progress_msg": msg}
    if prog is not None:
        d["progress"] = prog

    print(f"set_progress({task_id}), progress: {prog}, progress_msg: {msg}")
    try:
        TaskService.update_progress(task_id, d)
    except DoesNotExist:
        logging.warning(f"set_progress task {task_id} is unknown")
        if PAYLOAD:
            PAYLOAD.ack()
            PAYLOAD = None
        return
    
    close_connection()
    if cancel and PAYLOAD:
        PAYLOAD.ack()
        PAYLOAD = None
        raise TaskCanceledException(msg)


def collect():
    """
    参数: 无
    返回值: 如果成功获取到有效任务则返回该任务字典；否则返回None。
    功能: 从Redis队列中拉取未确认的消息作为新任务。首先尝试获取未确认的消息，若没有则作为消费者等待新消息到来。获取到消息后，检查对应的任务是否存在及是否已被取消，若无效则记录日志并返回None。
    """
    global CONSUMER_NAME, PAYLOAD, DONE_TASKS, FAILED_TASKS
    try:
        PAYLOAD = REDIS_CONN.get_unacked_for(CONSUMER_NAME, SVR_QUEUE_NAME, "mme__svr_task_broker")
        if not PAYLOAD:
            time.sleep(1)
            return None
    except Exception:
        logging.exception("Get task event from queue exception")
        return None
    
    msg = PAYLOAD.get_message()
    if not msg:
        return None
    
    task = None
    canceled = False
    try:
        task = TaskService.get_task(msg["id"])
        if task:
            _, file = FileService.get_by_id(task["file_id"])
            canceled = file.run == TaskStatus.CANCEL.value or file.progress < 0
    except DoesNotExist:
        pass
    except Exception:
        logging.exception("collect get_task exception")
    if not task or canceled:
        state = "is unknow" if not task else "has been cancelled"
        with mt_lock:
            DONE_TASKS += 1
        print(f"collect task {msg['id']} {state}")
        return None
    
    task["task_type"] = msg.get("task_type", "")
    return task


def get_storage_binary(bucket, name):
    return STORAGE_IMPL.get(bucket, name)

def init_kb(row, vector_size: int):
    collection_name = row.get("collection_name", "")
    if not collection_name:
        raise ValueError("collection_name is required")
    return settings.vectorDatabase.createCollection(collection_name, row.get("kb_id", ""), vector_size)


def do_handle_task(task):
    task_id = task["id"]
    task_embedding = task["embedding"]
    task_language = task["language"]

    progress_callback = partial(set_progress, task_id)

    try:
        task_canceled = TaskService.do_cancel(task_id)
    except DoesNotExist:
        logging.warning(f"task {task_id} is unknown")
        return
    if task_canceled:
        progress_callback(-1, msg="Task has been canceled.")
        return
    
    try:
        embedding_model = LLMBundle(LLMType.EMBEDDING, llm_name=task_embedding, lang=task_language)
        vts, _ = embedding_model.encode(["ok"])
        vector_size = len(vts[0])
    except Exception as e:
        error_message = f'Fail to bind embedding model: {str(e)}'
        progress_callback(-1, msg=error_message)
        logging.exception(error_message)
        raise
    
    init_kb(task, vector_size)

    if task.get("task_type", "") == "image":
        start_ts = timer()
        try:
            image_binary = get_storage_binary(task["bucket"], task["name"])
            v, c = embedding_model.encode([task.get("content", "")], [image_binary])
        except






def handle_task():
    global PAYLOAD, mt_lock, DONE_TASKS, FAILED_TASKS, CURRENT_TASK
    task = collect()
    if task:
        try:
            print(f"handle_task begin for task {json.dumps(task)}")
            with mt_lock:
                CURRENT_TASK = copy.deepcopy(task)
            do_handle_task(task)
            while mt_lock:
                DONE_TASKS += 1
                CURRENT_TASK = None
            print(f"handle_task done for task {json.dumps(task)}")
        except TaskCanceledException:
            with mt_lock:
                DONE_TASKS += 1
                CURRENT_TASK = None
            try:
                set_progress(task["id"], prog=-1, msg="handle_task got TaskCanceledException")
            except Exception:
                pass
            logging.debug("handle_task got TaskCanceledException", exc_info=True)
    if PAYLOAD:
        PAYLOAD.ack()
        PAYLOAD = None

