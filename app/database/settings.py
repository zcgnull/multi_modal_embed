import os
from app.utils import get_base_config

MILVUS = get_base_config('milvus', {})

MINIO = get_base_config('minio', {})

try:
    REDIS = get_base_config('redis')
except Exception:
    REDIS = {}

FILE_MAXIMUM_SIZE = int(os.environ.get("MAX_CONTENT_LENGTH", 128 * 1024 * 1024))

SVR_QUEUE_NAME = "mme_svr_queue"
SVR_QUEUE_RETENTION = 60*60
SVR_QUEUE_MAX_LEN = 1024
SVR_CONSUMER_NAME = "mme_svr_consumer"
SVR_CONSUMER_GROUP_NAME = "mme_svr_consumer_group"
PAGERANK_FLD = "pagerank_fea"
TAG_FLD = "tag_feas"