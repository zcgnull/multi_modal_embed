from abc import ABC
import base64
from io import BytesIO
import os
import PIL
from PIL import Image

from app.utils.model_utils import is_english
from app.utils.file_utils import get_project_base_directory
from app.utils import get_uuid

class Base(ABC):
    def __init__(self, key, model_name, base_url=None, **kwargs):
        pass

    def describe(self, image, max_token=300):
        raise NotImplementedError("Please implement encode method!")
    
    def chat(self, system, history, gen_conf, image=""):
        if system:
            history[-1["content"]] = system + history[-1]["content"] + "user query: " + history[-1]["content"]
        try:
            for his in history:
                if his["role"] == "user":
                    his["content"] = self.chat_prompt(his["content"], image)

            response = self.client.chat.completions.create(
                model=self._model_name,
                messages=history,
                max_otokens=gen_conf.get("max_tokens", 1000),
                temperature=gen_conf.get("temperature", 0.3),
                top_p=gen_conf.get("top_p", 0.7)
            )
            return response.choices[0].message.content.strip(), response.usage.total_tokens
        except Exception as e:
            return "**ERROR**: " + str(e), 0 

    def chat_stream(self, system, history, gen_conf, image=""):
        if system:
            history[-1]["content"] = system + history[-1]["content"] + "user query: " + history[-1]["content"]

        ans = ""
        tk_count = 0
        try:
            for his in history:
                if his["role"] == "user":
                    his["content"] = self.chat_prompt(his["content", image])

            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=history,
                max_tokens=gen_conf.get("max_tokens", 1000),
                temperature=gen_conf.get("temperature", 0.3),
                top_p=gen_conf.get("top_p", 0.7),
                stream=True
            )
            for resp in response:
                if not resp.choices[0].delta.content:
                    continue
                delta = resp.choices[0].delta.content
                ans += delta
                if resp.choices[0].finish_reason == "length":
                    ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                        [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
                    tk_count = resp.usage.total_tokens
                if resp.choices[0].finish_reason == "stop":
                    tk_count = resp.usage.total_tokens
                yield ans
        except Exception as e:
            yield ans + "\n**ERROR**: " + str(e)

        yield tk_count

    def image2base64(self, image):
        if isinstance(image, bytes):
            return base64.b64encode(image).decode('utf-8')
        if isinstance(image, BytesIO):
            return base64.b64encode(image.getvalue()).decode('utf-8')
        buffered = BytesIO()
        try:
            image.save(buffered, format="JPEG")
        except Exception:
            image.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode('utf-8')

    def prompt(self, b64):
        return [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{b64}"
                        },
                    },
                    {
                        "text": "请用中文详细描述一下图中的内容，比如时间，地点，人物，事情，人物心情等，如果有数据请提取出数据。" if self.lang.lower() == "chinese" else
                        "Please describe the content of this picture, like where, when, who, what happen. If it has number data, please extract them out.",
                    },
                ],
            }
        ]
            
    def chat_prompt(self, text, b64):
        return [
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{b64}",
                },
            },
            {
                "type": "text",
                "text": text
            },
        ]
    
class QWenVL(Base):
    def __init__(self, key, model_name, lang="Chinese", base_url=None, **kwargs):
        import dashscope
        dashscope.api_key = key
        self.model_name = model_name
        self.lang = lang

    def prompt(self, binary):
        tmp_dir = get_project_base_directory("tmp")
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        path = os.path.join(tmp_dir, "%s.jpg" % get_uuid())
        Image.open(BytesIO(binary)).save(path)
        return [
            {
                "role": "user",
                "content": [
                    {
                        "image": f"file://{path}"
                    },
                    {
                        "text": "请用中文详细描述一下图中的内容，比如时间，地点，人物，事情，人物心情等，如果有数据请提取出数据。" if self.lang.lower() == "chinese" else
                        "Please describe the content of this picture, like where, when, who, what happen. If it has number data, please extract them out.",
                    },
                ],
            }
        ]
    
    def chat_prompt(self, text, b64):
        return [
            {"image": f"{b64}"},
            {"text": text},
        ]
    
    def describe(self, image, max_token=300):
        from http import HTTPStatus
        from dashscope import MultiModalConversation
        response = MultiModalConversation.call(model=self.model_name,
                                               messages=self.prompt(image))
        if response.status_code == HTTPStatus.OK:
            return response.output.choices[0]['message']['content'][0]["text"], response.usage.output_tokens
        return response.message, 0

    def chat(self, system, history, gen_conf, image=""):
        from http import HTTPStatus
        from dashscope import MultiModalConversation
        if system:
            history[-1]["content"] = system + history[-1]["content"] + "user query: " + history[-1]["content"]

        for his in history:
            if his["role"] == "user":
                his["content"] = self.chat_prompt(his["content"], image)
        response = MultiModalConversation.call(model=self.model_name, messages=history,
                                               max_tokens=gen_conf.get("max_tokens", 1000),
                                               temperature=gen_conf.get("temperature", 0.3),
                                               top_p=gen_conf.get("top_p", 0.7))

        ans = ""
        tk_count = 0
        if response.status_code == HTTPStatus.OK:
            ans += response.output.choices[0]['message']['content']
            tk_count += response.usage.total_tokens
            if response.output.choices[0].get("finish_reason", "") == "length":
                ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                    [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
            return ans, tk_count

        return "**ERROR**: " + response.message, tk_count
    
    def chat_stream(self, system, history, gen_conf, image=""):
        from http import HTTPStatus
        from dashscope import MultiModalConversation
        if system:
            history[-1]["content"] = system + history[-1]["content"] + "user query: " + history[-1]["content"]

        for his in history:
            if his["role"] == "user":
                his["content"] = self.chat_prompt(his["content"], image)

        ans = ""
        tk_count = 0
        try:
            response = MultiModalConversation.call(model=self.model_name, messages=history,
                                                   max_tokens=gen_conf.get("max_tokens", 1000),
                                                   temperature=gen_conf.get("temperature", 0.3),
                                                   top_p=gen_conf.get("top_p", 0.7),
                                                   stream=True)
            for resp in response:
                if resp.status_code == HTTPStatus.OK:
                    ans = resp.output.choices[0]['message']['content']
                    tk_count = resp.usage.total_tokens
                    if resp.output.choices[0].get("finish_reason", "") == "length":
                        ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                            [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
                    yield ans
                else:
                    yield ans + "\n**ERROR**: " + resp.message if str(resp.message).find(
                        "Access") < 0 else "Out of credit. Please set the API key in **settings > Model providers.**"
        except Exception as e:
            yield ans + "\n**ERROR**: " + str(e)

        yield tk_count
