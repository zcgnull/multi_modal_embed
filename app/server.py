import logging
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import sys
from werkzeug.serving import run_simple
import traceback
import signal
import os

from app.constants import MME_VERSION
from app import settings 
from app.api import app

stop_event = threading.Event()

def update_progress():
    while not stop_event.is_set():
        try:
            stop_event.wait(6)
        except Exception:
            logging.exception("update_progress exception")

def signal_handler(sig, frame):
    logging.info("Received interrupt signal, shutting down...")
    stop_event.set()
    time.sleep(1)
    sys.exit(0)

if __name__ == '__main__':
    logging.info(r"""
     _      _      _____
    / \__/|/ \__/|/  __/
    | |\/||| |\/|||  \  
    | |  ||| |  |||  /_ 
    \_/  \|\_/  \|\____\
                    
    """)

    logging.info(
        f'MME version: {MME_VERSION}'
    )


    settings.init_settings()

    thread = ThreadPoolExecutor(max_workers=1)
    thread.submit(update_progress)

    # start http server
    try:
        logging.info("MME HTTP server start...")
        run_simple(
            hostname=settings.HOST_IP,
            port=settings.HOST_PORT,
            application=app,
            threaded=True,
            use_reloader=settings.DEBUG,
            use_debugger=settings.DEBUG,
        )
    except Exception:
        traceback.print_exc()
        stop_event.set()
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)