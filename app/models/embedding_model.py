from abc import ABC, abstractmethod
import threading
import re
import numpy as np
from http import HTTPStatus
import PIL.Image as Image
from io import BytesIO
import base64

from app.utils.file_utils import pil_to_fileobj
from app.utils.model_utils import num_tokens_from_string
from .settings import BAAI_VL_MODEL_PATH

class Base(ABC):
    def __init__(self, model_name):
        pass

    def encode(self, texts: list):
        raise NotImplementedError("Please implement encode method!")

    def encode_queries(self, text: str):
        raise NotImplementedError("Please implement encode method!")

    def total_token_count(self, resp):
        try:
            return resp.usage.total_tokens
        except Exception:
            pass
        try:
            return resp["usage"]["total_tokens"]
        except Exception:
            pass
        return 0
    
class BaaiVlEmbedding(Base):
    _model = None
    _model_name = ""
    _model_lock = threading.Lock()
    def __init__(self, model_path):
        with BaaiVlEmbedding._model_lock:
            from transformers import AutoModel

            BaaiVlEmbedding._model = AutoModel.from_pretrained(model_path, trust_remote_code=True) # You must set trust_remote_code=True
            match = re.search(r"/([a-zA-Z0-9_-]+)$", model_path)
            if match:
                result = match.group(1) 
                BaaiVlEmbedding._model_name = result
            BaaiVlEmbedding._model.set_processor(model_path)
            BaaiVlEmbedding._model.eval()
        self._model_name = BaaiVlEmbedding._model_name
        self._model = BaaiVlEmbedding._model

    def encode(self, texts: list, images: list):
        if texts is None and images is None:
            return np.array([]), 0
        
        if len(texts) != len(images) and len(texts) != 0 and len(images) != 0:
            raise Exception("The number of texts and images must be equal!")

        token_count = 0
        for t in texts:
            token_count += num_tokens_from_string(t)

        import torch
        with torch.no_grad():
            result = self._model.encode(
                images = images, 
                text = texts
            )

        return result.numpy(), token_count
    
    def encode_queries(self, text, image):
        if image:
            # 将二进制数据包装成 BytesIO
            img_stream = BytesIO(image)
            
            # 使用 PIL 打开图像
            try:
                pil_image = Image.open(img_stream)
                img_fileobj = pil_to_fileobj(pil_image)
            except Exception as e:
                raise Exception(f"Invalid image: {e}")
        token_count = num_tokens_from_string(text)
        return self._model.encode(text = [text] if text else None, images = [img_fileobj] if image else None).tolist()[0], token_count
    

class QwenMultiModelEmbed(Base):
    def __init__(self, key, model_name="multimodal-embedding-v1", **kwargs):
        self.model_name = model_name
        self.key = key

    def encode(self, texts, images, videos):
        import dashscope

        # 构建输入数据
        inputs = []
        for text, image, video in zip(texts, images, videos):
            input = {}
            if text is not None:
                input['text'] = text
            if image is not None:
                input['image'] = image
            if video is not None:
                input['video'] = video
            inputs.append(input)
        
        # 调用模型接口
        resp = dashscope.MultiModalEmbedding.call(
            model=self.model_name, 
            input=inputs,
            api_key=self.key
        )

        if resp.status_code == HTTPStatus.OK:
            res = []
            token_count = 0
            for t in texts:
                token_count += num_tokens_from_string(t)
            for embed in resp.output['embeddings']:
                res.append(embed['embedding'])

            return np.array(res), token_count
        
        else:
            return np.array([]), 0
        

    def encode_queries(self, text=None, image=None, image_format="png", video=None):
        input = {}
        if text is not None:
            input['text'] = text
        if image is not None:
            base64_image = base64.b64encode(image).decode('utf-8')
            # 设置图像格式
            image_data = f"data:image/{image_format};base64,{base64_image}"
            input['image'] = image_data
        if video is not None:
            input['video'] = video
        import dashscope
        resp = dashscope.MultiModalEmbedding.call(
            model=self.model_name, 
            input=[input],
            api_key=self.key
        )

        if resp.status_code == HTTPStatus.OK:
            token_count = num_tokens_from_string(text)
            return resp.output['embeddings'][0]['embedding'], token_count
        
        else:
            return np.array([]), 0
        
if __name__ == "__main__":
    baai = BaaiVlEmbedding(BAAI_VL_MODEL_PATH)
    texts = ["hello world"]
    images = ["/Users/zhaochenguang/Study/model/assets/cir_candi_1.png"]
    res, token_count = baai.encode(texts, images)
    print(res)
    print(token_count)