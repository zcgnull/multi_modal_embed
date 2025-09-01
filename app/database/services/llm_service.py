import json
import logging
import os

from app.database.db_models import DB, LLM
from app.database.services.commom_service import CommonService
from app.database import LLMType
from app.models import EmbeddingModel, TTSModel, ASRModel


class LLMService(CommonService):
    model = LLM

    @classmethod
    @DB.connection_context()
    def get_api_key(cls, tenant_id, model_name):
        mdlnm, fid = LLMService.split_model_name_and_factory(model_name)
        if not fid:
            objs = cls.query(llm_name=mdlnm)
        else:
            objs = cls.query(llm_name=mdlnm, llm_factory=fid)
        if not objs:
            return
        return objs[0]
    
    @classmethod
    @DB.connection_context()
    def get_model_config(cls, llm_type, llm_name=None):
        model_config = cls.get_api_key(llm_name)
        mdlnm, fid = LLMService.split_model_name_and_factory(llm_name)
        if model_config:
            model_config = model_config.to_dict()
        if not model_config:
            if llm_type in [LLMType.EMBEDDING, LLMType.RERANK]:
                llm = LLMService.query(llm_name=mdlnm) if not fid else LLMService.query(llm_name=mdlnm, fid=fid)
                if llm and llm[0].fid in ["BAAI"]:
                    model_config = {"llm_factory": llm[0].fid, "api_key": "", "llm_name": mdlnm, "api_base": ""}
            if not model_config:
                if mdlnm == "flag-embedding":
                    model_config = {"llm_factory": "Tongyi-Qianwen", "api_key": "",
                                    "llm_name": llm_name, "api_base": ""}
                else:
                    if not mdlnm:
                        raise LookupError(f"Type of {llm_type} model is not set.")
                    raise LookupError("Model({}) not authorized".format(mdlnm))
        return model_config
    
    @classmethod
    @DB.connection_context()
    def model_instance(cls, llm_type, llm_name=None, lang="Chinese"):
        model_config = LLMService.get_model_config(llm_type, llm_name)
        if llm_type == LLMType.EMBEDDING.value:
            if model_config["llm_factory"] not in EmbeddingModel:
                return
            if "config" in model_config:
                config = json.loads(model_config["config"])
            else:
                config = {}
            return EmbeddingModel[model_config["llm_factory"]](
                api_key=model_config["api_key"], model_name=model_config["llm_name"], base_url=model_config["api_base"], model_path=config["model_path"] if "model_path" in config else None)

        if llm_type == LLMType.ASR:
            if model_config["llm_factory"] not in ASRModel:
                return
            return ASRModel[model_config["llm_factory"]](
                key=model_config["api_key"], model_name=model_config["llm_name"],
                lang=lang,
                base_url=model_config["api_base"]
            )
        if llm_type == LLMType.TTS:
            if model_config["llm_factory"] not in TTSModel:
                return
            return TTSModel[model_config["llm_factory"]](
                model_config["api_key"],
                model_config["llm_name"],
                base_url=model_config["api_base"],
            )
        
    @classmethod
    @DB.connection_context()
    def increase_usage(cls, llm_type, used_tokens, llm_name=None):
        llm_name, llm_factory = LLMService.split_model_name_and_factory(llm_name)

        try:
            num = cls.model.update(
                used_tokens=cls.model.used_tokens + used_tokens
            ).where(
                cls.model.llm_name == llm_name,
                cls.model.llm_factory == llm_factory if llm_factory else True
            ).execute()
        except Exception:
            logging.exception(
                "TenantLLMService.increase_usage got exception,Failed to update used_tokens for llm_name=%s",
                llm_name)
            return 0

        return num

    @staticmethod
    def split_model_name_and_factory(model_name):
        arr = model_name.split("@")
        if len(arr) < 2:
            return model_name, None
        if len(arr) > 2:
            return "@".join(arr[0:-1]), arr[-1]


class LLMBundle(object):
    def __init__(self, llm_type, llm_name=None, lang="Chinese"):
        self.llm_type = llm_type
        self.llm_name = llm_name
        self.mdl = LLMService.model_instance(
            llm_type, llm_name, lang=lang)
        assert self.mdl, "Can't find model for {}/{}/{}".format(
            llm_type, llm_name)
        model_config = LLMService.get_model_config(llm_type, llm_name)
        self.max_length = model_config.get("max_tokens", 8192)

    def encode(self, file):
        embeddings, used_tokens = self.mdl.encode(file)
        if not LLMService.increase_usage(
                self.llm_type, used_tokens):
            logging.error(
                "LLMBundle.encode can't update token usage for EMBEDDING used_tokens: {}".format(used_tokens))
        return embeddings, used_tokens
    
    def encode_queries(self, query: str):
        emd, used_tokens = self.mdl.encode_queries(query)
        if not LLMService.increase_usage(
                self.llm_type, used_tokens):
            logging.error(
                "LLMBundle.encode_queries can't update token usage for EMBEDDING used_tokens: {}".format(used_tokens))
        return emd, used_tokens

    def asr(self, audio):
        txt, used_tokens = self.mdl.asr(audio)
        if not LLMService.increase_usage(
                self.llm_type, used_tokens):
            logging.error(
                "LLMBundle.transcription can't update token usage for ASR used_tokens: {}".format(used_tokens))
        return txt

    def tts(self, text):
        for chunk in self.mdl.tts(text):
            if isinstance(chunk, int):
                if not LLMService.increase_usage(
                        self.llm_type, chunk, self.llm_name):
                    logging.error(
                        "LLMBundle.tts can't update token usage for TTS".format())
                return
            yield chunk

    def chat(self, system, history, gen_conf):
        txt, used_tokens = self.mdl.chat(system, history, gen_conf)
        if isinstance(txt, int) and not LLMService.increase_usage(
                self.llm_type, used_tokens, self.llm_name):
            logging.error(
                "LLMBundle.chat can't update token usage for CHAT llm_name: {}, used_tokens: {}".format(self.llm_name,
                                                                                                           used_tokens))
        return txt

    def chat_streamly(self, system, history, gen_conf):
        for txt in self.mdl.chat_streamly(system, history, gen_conf):
            if isinstance(txt, int):
                if not LLMService.increase_usage(
                        self.llm_type, txt, self.llm_name):
                    logging.error(
                        "LLMBundle.chat_streamly can't update token usage for CHAT llm_name: {}, content: {}".format(self.llm_name,
                                                                                                                        txt))
                return
            yield txt