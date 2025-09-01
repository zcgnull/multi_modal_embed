from datetime import datetime
import random
import os

from app.database.services.commom_service import CommonService
from app.database.services.file_service import FileService
from app.database.db_models import Task, File, Knowledgebase, DB
from app.database import TaskStatus, FileType
from app.database.redis_database import REDIS_CONN
from app.database.settings import SVR_QUEUE_NAME
from app.utils.db_utils import bulk_insert_into_db
from app.utils import get_uuid

def trim_header_by_lines(text: str, max_length) -> str:
    len_text = len(text)
    if len_text <= max_length:
        return text
    for i in range(len_text):
        if text[i] == '\n' and len_text - i <= max_length:
            return text[i + 1:]
    return text

class TaskService(CommonService):
    model = Task

    @classmethod
    @DB.connection_context()
    def get_task(cls, task_id):
        fields = [
            cls.model.id,
            cls.model.file_id,
            cls.model.retry_count,
            File.id,
            File.kb_id,
            File.name,
            File.size,
            File.content,
            File.type,
            File.parser_config,
            File.parser_type,
            Knowledgebase.model,
            Knowledgebase.parser_config,
            cls.model.update_time
        ]
        files = (
            cls.model.select(*fields)
                .join(File, on=(cls.model.file_id == File.id))
                .join(Knowledgebase, on=(File.kb_id == Knowledgebase.id))
                .where(cls.model.id == task_id)
        )
        files = list(files.dicts())
        if not files:
            return None
        
        msg = f"\n{datetime.now().strftime('%H:%M:%S')} Task has been received."
        prog = random.random() / 10.0
        if files[0]["retry_count"] >= 3:
            msg = "\nERROR: Task is abandoned after 3 times attempts."
            prog = -1

        cls.model.update(
            progress_msg=cls.model.progress_msg + msg,
            progress=prog,
            retry_count=files[0]["retry_count"] + 1,
        ).where(cls.model.id == files[0]["id"]).execute()

        if files[0]["retry_count"] >= 3:
            return None
        
        return files[0]
    
    @classmethod
    @DB.connection_context()
    def get_tasks(cls, file_id: str):
        fields = [
            cls.model.id,   
            cls.model.progress,
        ]
        tasks = (
            cls.model.select(*fields).order_by(cls.model.create_time.desc())
                .where(cls.model.file_id == file_id)
        )
        tasks = list(tasks.dicts())
        if not tasks:
            return None
        return tasks
    
    @classmethod
    @DB.connection_context()
    def do_cancel(cls, id):
        task = cls.model.get_by_id(id)
        _, file = FileService.get_by_id(task.doc_id)
        return file.run == TaskStatus.CANCEL.value or file.progress < 0

    @classmethod
    @DB.connection_context()
    def update_progress(cls, id, info):
        if os.environ.get("MACOS"):
            if info["progress_msg"]:
                task = cls.model.get_by_id(id)
                progress_msg = trim_header_by_lines(task.progress_msg + "\n" + info["progress_msg"], 3000)
                cls.model.update(progress_msg=progress_msg).where(cls.model.id == id).execute()
            if "progress" in info:
                cls.model.update(progress=info["progress"]).where(
                    cls.model.id == id
                ).execute()
            return
        
        with DB.lock("update_progress", -1):
            if info["progress_msg"]:
                task = cls.model.get_by_id(id)
                progress_msg = trim_header_by_lines(task.progress_msg + "\n" + info["progress_msg"], 3000)
                cls.model.update(progress_msg=progress_msg).where(cls.model.id == id).execute()
            if "progress" in info:
                cls.model.update(progress=info["progress"]).where(
                    cls.model.id == id
                ).execute()
    
def queue_tasks(file: dict, bucket: str, name: str):
    def new_task():
        return {"id": get_uuid(), "file_id": file["id"], "progress": 0.0}
    
    parse_task_array = []

    if file["type"] == FileType.IMAGE.value:
        task = new_task()
        parse_task_array.append(task)

    bulk_insert_into_db(Task, parse_task_array, True)

    unfinished_task_array = [task for task in parse_task_array if task["progress"] < 1.0]
    for unfinished_task in unfinished_task_array:
        assert REDIS_CONN.queue_product(
            SVR_QUEUE_NAME, messages=unfinished_task
        ), "Can't access Redis. Please check the Redis' status."
    
