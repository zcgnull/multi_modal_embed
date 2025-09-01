import os
from enum import IntEnum, Enum

from app.utils import get_base_config
from app.constants import SERVICE_NAME
import app.database.milvus_database


EMBEDDING_MDL = ""
RERANK_MDL = ""
VECTOR_ENGINE = ""
HOST_IP = None
HOST_PORT = None
DEBUG = False

DATABASE_TYPE = os.getenv("DB_TYPE", 'mysql')
DATABASE = get_base_config(DATABASE_TYPE)

vectorDatabase = None

def init_settings():

    global DATABASE_TYPE, DATABASE
    DATABASE_TYPE = os.getenv("DB_TYPE", 'mysql')
    DATABASE = get_base_config(DATABASE_TYPE)

    global EMBEDDING_MDL, RERANK_MDL
    EMBEDDING_MDL = os.getenv("EMBEDDING_MDL", "")
    RERANK_MDL = os.getenv("RERANK_MDL", "")

    global vectorDatabase, VECTOR_ENGINE
    VECTOR_ENGINE = os.getenv("VECTOR_ENGINE", "milvus")
    lower_case_doc_engine = VECTOR_ENGINE.lower()
    if lower_case_doc_engine == "milvus":
        from app.database.milvus_database import MilvusDatabase
        vectorDatabase = MilvusDatabase()
    else:
        raise Exception(f"Not supported vector engine: {VECTOR_ENGINE}")

    global HOST_IP, HOST_PORT, DEBUG
    HOST_IP = get_base_config(SERVICE_NAME, {}).get("host", "127.0.0.1")
    HOST_PORT = get_base_config(SERVICE_NAME, {}).get("port", 9090)
    DEBUG = get_base_config(SERVICE_NAME, {}).get("debug", False)


class CustomEnum(Enum):
    @classmethod
    def valid(cls, value):
        try:
            cls(value)
            return True
        except BaseException:
            return False

    @classmethod
    def values(cls):
        return [member.value for member in cls.__members__.values()]

    @classmethod
    def names(cls):
        return [member.name for member in cls.__members__.values()]

class RetCode(IntEnum, CustomEnum):
    SUCCESS = 0
    NOT_EFFECTIVE = 10
    EXCEPTION_ERROR = 100
    ARGUMENT_ERROR = 101
    DATA_ERROR = 102
    OPERATING_ERROR = 103
    CONNECTION_ERROR = 105
    RUNNING = 106
    PERMISSION_ERROR = 108
    AUTHENTICATION_ERROR = 109
    UNAUTHORIZED = 401
    SERVER_ERROR = 500
    FORBIDDEN = 403
    NOT_FOUND = 404