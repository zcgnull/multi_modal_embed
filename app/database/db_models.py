import os
import operator
import typing
import logging
from functools import wraps
from enum import Enum
from peewee import Model, BigIntegerField, DateTimeField, CompositeKey, IntegerField, FloatField, Field, CharField, TextField
from playhouse.pool import PooledMySQLDatabase, PooledPostgresqlDatabase
from playhouse.db_url import DatabaseProxy

from app import settings
from app import utils

def singleton(cls, *args, **kwargs):
    instances = {}
    
    def _singleton():
        key = str(cls) + str(os.getpid())
        if key not in instances:
            instances[key] = cls(*args, **kwargs)
        return instances[key]
    
    return _singleton


CONTINUOUS_FIELD_TYPE = {IntegerField, FloatField, DateTimeField}
AUTO_DATE_TIMESTAMP_FIELD_PREFIX = {
    "create",
    "start",
    "end",
    "update",
    "read_access",
    "write_access"}

def is_continuous_field(cls: typing.Type) -> bool:
    if cls in CONTINUOUS_FIELD_TYPE:
        return True
    for p in cls.__bases__:
        if p in CONTINUOUS_FIELD_TYPE:
            return True
        elif p is not Field and p is not object:
            if is_continuous_field(p):
                return True
    else:
        return False

def auto_date_timestamp_field():
    return {f"{f}_time" for f in AUTO_DATE_TIMESTAMP_FIELD_PREFIX}


def auto_date_timestamp_db_field():
    return {f"f_{f}_time" for f in AUTO_DATE_TIMESTAMP_FIELD_PREFIX}


def remove_field_name_prefix(field_name):
    return field_name[2:] if field_name.startswith('f_') else field_name


class BaseModel(Model):
    create_time = BigIntegerField(null=True, index=True)
    create_date = DateTimeField(null=True, index=True)
    update_time = BigIntegerField(null=True, index=True)
    update_date = DateTimeField(null=True, index=True)

    def to_json(self):
        return self.to_dict()
    
    def to_dict(self):
        return self.__data__['__dict__']
    
    @property
    def meta(self):
        return self._meta
    
    @classmethod
    def get_primary_keys_name(cls):
        return cls._meta.primary_key.field_names if isinstance(cls._meta.primary_key, CompositeKey) else [
            cls._meta.primary_key.name]
    
    @classmethod
    def getter_by(cls, attr):
        return operator.attrgetter(attr)(cls)
    
    @classmethod
    def query(cls, reverse=None, order_by=None, **kwargs):
        filters = []
        for f_n, f_v in kwargs.items():
            attr_name = '%s' % f_n
            if not hasattr(cls, attr_name) or f_v is None:
                continue
            if type(f_v) in {list, set}:
                f_v = list(f_v)
                if is_continuous_field(type(getattr(cls, attr_name))):
                    if len(f_v) == 2:
                        for i, v in enumerate(f_v):
                            if isinstance(v, str) and f_n in auto_date_timestamp_field():
                                 f_v[i] = utils.date_string_to_timestamp(v)
                        lt_value = f_v[0]
                        gt_value = f_v[1]
                        if lt_value is not None and gt_value is not None:
                            filters.append(cls.getter_by(attr_name).between(lt_value, gt_value))
                        elif lt_value is not None:
                            filters.append(operator.attrgetter(attr_name)(cls) >= lt_value)
                        elif gt_value is not None:
                            filters.append(operator.attrgetter(attr_name)(cls) <= gt_value)
                else:
                    filters.append(operator.attrgetter(attr_name)(cls) << f_v)
            else:
                filters.append(operator.attrgetter(attr_name)(cls) == f_v)
        if filters:
            query_records = cls.select().where(*filters)
            if reverse is not None:
                if not order_by or not hasattr(cls, f"{order_by}"):
                    order_by = "create_time"
                if reverse is True:
                    query_records = query_records.order_by(cls.getter_by(f"{order_by}").desc())
                elif reverse is False:
                    query_records = query_records.order_by(cls.getter_by(f"{order_by}").asc())
            return [query_record for query_record in query_records]
        else:
            return []
        

    @classmethod
    def insert(cls, __data=None, **insert):
        if isinstance(__data, dict) and __data:
            __data[cls.__meta.combined["create_time"]] = utils.current_timestamp()
        if insert:
            insert["create_time"] = utils.current_timestamp()

        return super().insert(__data, **insert)
    
    @classmethod
    def _normalize_data(cls, data, kwargs):
        normalized = super()._normalize_data(data, kwargs)
        if not normalized:
            return {}
        
        normalized[cls._meta.combined["update_time"]] = utils.current_timestamp()

        for f_n in AUTO_DATE_TIMESTAMP_FIELD_PREFIX:
            if {f"{f_n}_time", f"{f_n}_date"}.issubset(cls._meta.combined.keys()) and \
                    cls._meta.combined[f"{f_n}_time"] in normalized and \
                    normalized[cls._meta.combined[f"{f_n}_time"]] is not None:
                normalized[cls._meta.combined[f"{f_n}_date"]] = utils.timestamp_to_date(
                    normalized[cls._meta.combined[f"{f_n}_time"]])
        return normalized

class PooledDatabase(Enum):
    MYSQL = PooledMySQLDatabase
    POSTGRES = PooledPostgresqlDatabase

@singleton
class BaseDataBase:
    def __init__(self):
        database_config = settings.DATABASE.copy()
        db_name = database_config.pop("name")
        self.database_connection = PooledDatabase[settings.DATABASE_TYPE.upper()].value(db_name, **database_config)
        logging.info('init database on cluster mode successfully')


class PostgresDatabaseLock:
    def __init__(self, lock_name, timeout=10, db=None):
        self.lock_name = lock_name
        self.timeout = timeout
        self.db = db if db else DB 

    def lock(self):
        cursor = self.db.execute_sql("SELECT pg_try_advisory_lock(%s)", self.timeout)
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f'acquire postgres lock {self.lock_name} timeout')
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f'failed to acquire lock {self.lock_name}')
        
    def unlock(self):
        cursor = self.db.execute_sql("SELECT pg_advisory_unlock(%s)", self.timeout)
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(
                f'postgres lock {self.lock_name} was not established by this thread')
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f'postgres lock {self.lock_name} does not exist')
        
    def __enter__(self):
        if isinstance(self.db, PostgresDatabaseLock):
            self.lock()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(self.db, PostgresDatabaseLock):
            self.unlock()

    def __call__(self, func):
        @wraps(func)
        def magic(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        
        return magic
    
class MysqlDatabaseLock:
    def __init__(self, lock_name, timeout=10, db=None):
        self.lock_name = lock_name
        self.timeout = int(timeout)
        self.db = db if db else DB

    def lock(self):
        cursor = self.db.execute_sql(
            "SELECT GET_LOCK(%s, %s)", (self.lock_name, self.timeout))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f'acquire mysql lock {self.lock_name} timeout')
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f'failed to acquire lock {self.lock_name}')
        
    def unlock(self):
        cursor = self.db.execute_sql(
            "SELECT RELEASE_LOCK(%s)", (self.lock_name,))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(
                f'mysql lock {self.lock_name} was not established by this thread')
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f'mysql lock {self.lock_name} does not exist')

    def __enter__(self):
        if isinstance(self.db, PooledMySQLDatabase):
            self.lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(self.db, PooledMySQLDatabase):
            self.unlock()

    def __call__(self, func):
        @wraps(func)
        def magic(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return magic
    
class DatabaseLock(Enum):
    MYSQL = MysqlDatabaseLock
    POSTGRES = PostgresDatabaseLock

db_proxy = DatabaseProxy()

DB = BaseDataBase().database_connection
DB.lock = DatabaseLock[settings.DATABASE_TYPE.upper()].value

def close_connection():
    try:
        if DB:
            DB.close_stale(age=30)
    except Exception as e:
        logging.exception(e)

class DataBaseModel(BaseModel):
    class Meta:
        database = db_proxy
        db_table = "test"

db_proxy.initialize(DB)

def with_database(db_type: str):
    """
    Decorator to temporarily switch the database connection type for a function.
    
    Args:
        db_type (str): The database type to switch to (e.g., "mysql" or "postgres").
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            original_db = db_proxy.database
            try:
                database_config = settings.DATABASE[db_type.upper()].copy()
                db_name = database_config.pop("name")
                new_db = PooledDatabase[db_type.upper()].value(db_name, **database_config)
                db_proxy.initialize(new_db)
                result = func(*args, **kwargs)
                return result
            finally:
                db_proxy.initialize(original_db)
        return wrapper
    return decorator

class Knowledgebase(DataBaseModel):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=100)
    bucket = CharField(max_length=100)
    collection = CharField(max_length=100)
    model = TextField()
    parser_config = TextField()

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "knowledgebase"

class File(DataBaseModel):
    id = BigIntegerField(primary_key=True)
    kb_id = BigIntegerField()
    name = CharField(max_length=255)
    location = CharField(max_length=255)
    size = IntegerField()
    content = TextField()
    parser_config = TextField()
    parser_type = CharField(max_length=100)
    type = CharField(max_length=100)
    source_type = CharField(max_length=100)
    run = CharField(max_length=100)

    def __str__(self):
        return self.name
    
    class Meta:
        db_table = "file"

class Task(DataBaseModel):
    id = BigIntegerField(primary_key=True)
    file_id = BigIntegerField()
    progress = FloatField()
    progress_msg = TextField(
        null=True,
        help_text="process message",
        default="")
    retry_count = IntegerField()
    type = CharField(max_length=100)
    
    class Meta:
        db_table = "task"

class LLM(DataBaseModel):
    id = BigIntegerField(primary_key=True)
    llm_factory = CharField(max_length=100)
    model_type = CharField(max_length=100)
    llm_name = CharField(max_length=100)
    api_key = CharField(max_length=1024)
    base_url = CharField(max_length=255)
    config = TextField()
    max_tokens = IntegerField()
    used_tokens = IntegerField()

    def __str__(self):
        return self.llm_name
    
    class Meta:
        db_table = "llm"