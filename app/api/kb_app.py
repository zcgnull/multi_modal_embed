import re
from flask import request
import logging
import uuid
import PIL.Image as Image
from io import BytesIO

from app import settings
from app.utils.api_utils import validate_request
from app.utils.api_utils import get_json_result
from app.utils.file_utils import pil_to_fileobj
from app.database.services.knowledgebase_service import KnowledgebaseService
from app.database.storage_factory import STORAGE_IMPL, STORAGE_IMPL_TYPE
from app.models import EmbeddingModelFactory
from app.models.settings import BAAI_VL_MODEL_PATH

@manager.route('/list', methods=['GET'])
def list_knowledge_base():
    """
    列出所有知识库
    """
    kbs = KnowledgebaseService.get_all()
    result= []
    for kb in kbs:
        result.append({
            "kb_id": kb.id, 
            "kb_name": kb.name,
            "bucket": kb.bucket,
            "collection": kb.collection,
            "model": kb.model
            })

    return get_json_result(data=result)

@manager.route('/create', methods=['POST'])
@validate_request("kb_name", "vector_size", "model")
def create_knowledge_base():
    """
    创建知识库
    """
    kb_name = request.form.get("kb_name")
    model = request.form.get("model")
    try:
        vector_size = int(request.form.get("vector_size"))
    except (TypeError, ValueError):
        return get_json_result(code=settings.RetCode.ARGUMENT_ERROR, message="vector_size must be an integer")

    # kb_name必须为英文和数字组成
    if not re.match(r'^[a-zA-Z0-9]+$', kb_name):
        return get_json_result(code=settings.RetCode.ARGUMENT_ERROR, message="knowledge base name must be composed of English and numbers")
    kb = KnowledgebaseService.get_or_none(name=kb_name)
    if kb:
        return get_json_result(code=settings.RetCode.DATA_ERROR, message=f"knowledge base {kb_name} already exists", data=False)
    
    try:
        uid = uuid.uuid4().hex
        bucket_name = f'{STORAGE_IMPL_TYPE.lower()}-{kb_name}-{uid[:8]}'
        collection_name = f'{settings.VECTOR_ENGINE.lower()}_{kb_name}_{uid[:8]}'

        STORAGE_IMPL.conn.make_bucket(bucket_name)

        settings.vectorDatabase.createCollection(collectionName=collection_name, vectorSize=vector_size, knowledgebaseId='')
        
        kb = {
            "name": kb_name,
            "bucket": bucket_name,
            "collection": collection_name,
            "model": model
        }
        kb = KnowledgebaseService.insert(**kb)
    except Exception as e:
        return get_json_result(code=settings.RetCode.EXCEPTION_ERROR, message=str(e))
    
    return get_json_result(data=kb)

@manager.route('/insert', methods=['POST'])
@validate_request("kb_id")
def insert_multi_model_data():
    """
        插入多模态数据（单条）到知识库中

        todo:暂时只支持图片和文本两种模态数据
    """
    kb_id = request.form.get("kb_id")
    text = request.form.get("text", "")
    image = request.files.get("image")
    video = request.files.get("video")
    if not image:
        return get_json_result(code=settings.RetCode.ARGUMENT_ERROR, message="image is required")
    kb = KnowledgebaseService.get_or_none(id=kb_id)
    if kb:
        if image:
            # 图片数据
            # 读取二进制数据
            img_bytes = image.read()
            
            # 保存图片到对象存储中
            pic_name = f'{uuid.uuid4().hex}.{image.filename.split('.')[-1]}'
            STORAGE_IMPL.put(bucket=kb.bucket, fnm=pic_name, binary=img_bytes)
            
        if video:
            # 视频数据，
            pass
        # 模型处理图片数据并插入到向量数据库中
        if kb.model == "BaaiVl":
            embed_model = EmbeddingModelFactory[kb.model](BAAI_VL_MODEL_PATH)
            v, _ = embed_model.encode_queries(text if text else None, img_bytes)
        elif kb.model == "Qwen":
            embed_model = EmbeddingModelFactory[kb.model](key="sk-83e82632fcca46b388b454c5efa116fa", model="multimodal-embedding-v1")
            v, _ = embed_model.encode_queries(text if text else None, img_bytes, image.mimetype.split("/")[-1])
        else:
            return get_json_result(message=f'model {kb.model} not support')
        settings.vectorDatabase.insert(collection_name=kb.collection, data=[{"vector": v, "bucket": kb.bucket, "file_name": pic_name, "text": text}])
        return get_json_result(message="success")
    else:
        return get_json_result(message=f'kb {kb_id} is not exists')
