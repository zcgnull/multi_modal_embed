import re
from flask import request
import logging
import uuid
import PIL.Image as Image
from io import BytesIO
import json

from app import settings
from app.utils.api_utils import validate_request
from app.utils.api_utils import get_json_result
from app.utils.file_utils import pil_to_fileobj
from app.database.services.knowledgebase_service import KnowledgebaseService
from app.database.storage_factory import STORAGE_IMPL, STORAGE_IMPL_TYPE, STORAGE_URL
from app.models import EmbeddingModelFactory
from app.models.settings import BAAI_VL_MODEL_PATH


@manager.route('/retrieval', methods=['POST'])
@validate_request("kb_id",)
def retrieval():
    """
        在知识库中进行检索

        todo:暂时只支持图片和文本两种模态数据
    """
    kb_id = request.form.get("kb_id")
    text = request.form.get("text", "")
    image = request.files.get("image")
    top_k = int(request.form.get("top_k", 5))
    score = float(request.form.get("score", 0.2))
    
    kb = KnowledgebaseService.get_or_none(id=kb_id)
    if kb:
        vector = []
        if image:
            # 图片数据
            # 读取二进制数据
            img_bytes = image.read()
                
                # 模型处理图片数据
        if kb.model == "BaaiVl":
            embed_model = EmbeddingModelFactory[kb.model](BAAI_VL_MODEL_PATH)
            v, _ = embed_model.encode_queries(text if text else None, img_bytes if image else None)
            vector = [v]
        elif kb.model == "Qwen":
            embed_model = EmbeddingModelFactory[kb.model](key="sk-83e82632fcca46b388b454c5efa116fa", model="multimodal-embedding-v1")
            v, _ = embed_model.encode_queries(text if text else None, img_bytes if image else None, image.mimetype.split("/")[-1] if image else None)
            vector = [v]
        else:
            return get_json_result(message=f'model {kb.model} not support')
        
        # 在向量数据库中进行检索
        results = settings.vectorDatabase.search(collection_name=kb.collection, data=vector, limit=top_k, output_fields=["bucket", "file_name", "text"], search_params={"metric_type": "IP"})
        if results:
            retrieval_data = []
            for result in results:
                for hit in result:
                    if hit.distance >= score:
                        retrieval_data.append({
                            "id": hit.id,
                            "score": hit.distance,
                            "bucket": hit.bucket,
                            "file_name": hit.file_name,
                            "text": hit.text,
                            "url": f"{STORAGE_URL}/{hit.bucket}/{hit.file_name}"
                        })
            return get_json_result(message=f'success', data=retrieval_data)
        else:
            return get_json_result(message=f'No matching data found')

    return get_json_result(message=f'insert success')

