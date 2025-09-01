import os
from enum import Enum

from app.database.minio_database import MinioDatabase
from app.database import settings

class Storage(Enum):
    MINIO = 1

class StorageFactory:
    storage_mapping = {
        Storage.MINIO: MinioDatabase,
    }

    @classmethod
    def create(cls, storage: Storage):
        return cls.storage_mapping[storage]()
    
class UrlFactory:

    @classmethod
    def create(cls, storage: Storage):
        if storage == Storage.MINIO:
            url = ''
            if settings.MINIO["ssl"]:
                url = f'https://{settings.MINIO["host"]}'
            else:
                url = f'http://{settings.MINIO["host"]}'
            return url
        

    
STORAGE_IMPL_TYPE = os.getenv('STORAGE_IMPL', 'MINIO')
STORAGE_IMPL = StorageFactory.create(Storage[STORAGE_IMPL_TYPE.upper()])

STORAGE_URL = UrlFactory.create(Storage[STORAGE_IMPL_TYPE.upper()])
