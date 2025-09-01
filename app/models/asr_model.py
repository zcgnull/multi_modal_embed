from abc import ABC
import os
import io
import json
from http import HTTPStatus
from urllib import request

from app.settings import CustomEnum
from app.utils.file_utils import save_temp_file

class ASRTypeEnum(CustomEnum):
    COMMON = "common"
    PARAFORMER = "paraformer"
    SENSEVOICE = "sensevoice"
    FUNASR = "funasr"



class Base(ABC):
    def __init__(self, model_name: str, key, base_url, **kwargs):
        pass

    def asr(self, audio, **kwargs):
        pass

    def asr_stream(self, audio, **kwargs):
        pass


class QwenASR(Base):
    '''
    模型选型建议

    语种支持：
        1.  - 对于中文（普通话）、英语，建议优先选择通义千问ASR或Paraformer（最新版Paraformer-v2）模型以获得更优效果。
            - 对于中文（方言）、粤语、日语、韩语、西班牙语、印尼语、法语、德语、意大利语、马来语，建议优先选择Paraformer模型。特别是最新版Paraformer-v2模型，它支持指定语种，包括中文（含普通话和多种方言）、粤语、英语、日语、韩语。指定语种后，系统能够集中算法资源和语言模型于该特定语种，避免了在多种可能的语种中进行猜测和切换，从而减少了误识别的概率。
            - 对于其他语言（俄语、泰语等），请选择SenseVoice，具体请参见Java SDK。
        2. 文件读取方式：Paraformer、SenseVoice和通义千问ASR模型均支持读取录音文件的URL，如果需要读取本地录音文件，请选择通义千问ASR模型。
        3. 热词定制：如果您的业务领域中，有部分专有名词和行业术语识别效果不够好，您可以定制热词，将这些词添加到词表从而改善识别结果。如需使用热词功能，请选择Paraformer模型。关于热词的更多信息，Paraformer v1系列模型请参见Paraformer语音识别热词定制与管理，Paraformer v2及更高版本模型请参见定制热词。
        4. 时间戳：如果您需要在获取识别结果的同时获取时间戳，请选择Paraformer或者SenseVoice模型。
        5. 情感和事件识别：如果需要情感识别能力（包括高兴<HAPPY>、伤心<SAD>、生气<ANGRY>和中性<NEUTRAL>）和4种常见音频事件识别（包括背景音乐<BGM>、说话声<Speech>、掌声<Applause>和笑声<Laughter>），请选择SenseVoice语音识别模型。
        6. 流式输出：如果需要实现边推理边输出内容的流式输出效果，请选择通义千问ASR模型。
    '''
    def __init__(self, model_name: str, key, base_url, **kwargs):
        import dashscope

        dashscope.api_key = key
        self.model_name = model_name

    def asr(self, audio, **kwargs):
        from dashscope import MultiModalConversation
        from dashscope.audio.asr import Recognition
        # 判断是否是有type参数
        if 'type' in kwargs:
            asr_type = kwargs['type']
            if not ASRTypeEnum.valid(asr_type):
                raise ValueError(f"Invalid ASR type: {asr_type}")
            asr_type = ASRTypeEnum(asr_type)
        else:
            asr_type = ASRTypeEnum.COMMON

        match asr_type:
            case ASRTypeEnum.COMMON:
                # 判断audio是否是文件路径
                if isinstance(audio, str) and os.path.isfile(audio):
                    audio_file_path = f"file://{audio}"
                    messages = [
                        {
                            "role": "user",
                            "content": [{"audio": audio_file_path}],
                        }
                    ]
                elif isinstance(audio, str) and audio.startswith("http"):
                    messages = [
                        {
                            "role": "user",
                            "content": [
                                {"audio": audio},
                            ]
                        }
                    ]
                else:
                    raise ValueError("audio must be a file path or an audio URL.")
                response = MultiModalConversation.call(
                            model=self.model_name,
                            messages=messages)
                return response.output.choices[0]['message']['content'][0]["text"]
            case ASRTypeEnum.PARAFORMER:
                recognition = Recognition(
                    model='paraformer-realtime-v2',
                    format='wav',
                    sample_rate=16000,
                    # “language_hints”只支持paraformer-realtime-v2模型
                    language_hints=kwargs.get("language", ["zh", "en"]),
                    callback=None)
                if not(isinstance(audio, str) and os.path.isfile(audio)):
                    audio = save_temp_file(audio)
                    flag = True
                result = recognition.call(audio)
                if flag:
                    os.remove(audio)
                if result.status_code == HTTPStatus.OK:
                    return result.get_sentence()[0]['text']
            case ASRTypeEnum.SENSEVOICE:
                raise NotImplementedError("SenseVoice ASR type is not implemented yet.")
            case ASRTypeEnum.FUNASR:
                raise NotImplementedError("FunASR type is not implemented yet.")

    def asr_stream(self, audio, **kwargs):
        # 判断是否是有type参数
        if 'type' in kwargs:
            asr_type = kwargs['type']
            if not ASRTypeEnum.valid(asr_type):
                raise ValueError(f"Invalid ASR type: {asr_type}")
            asr_type = ASRTypeEnum(asr_type)
        else:
            asr_type = ASRTypeEnum.COMMON

        match asr_type:
            case ASRTypeEnum.COMMON:
                raise NotImplementedError("Common ASR type is not implemented yet.")
            case ASRTypeEnum.PARAFORMER:
                raise NotImplementedError("Paraformer ASR type is not implemented yet.")
            case ASRTypeEnum.SENSEVOICE:
                raise NotImplementedError("SenseVoice ASR type is not implemented yet.")
            

            
if __name__ == "__main__":
    asr = QwenASR(model_name="paraformer-realtime-v1", key="sk-83e82632fcca46b388b454c5efa116fa", base_url="")
    audio = "https://dashscope.oss-cn-beijing.aliyuncs.com/samples/audio/paraformer/hello_world_female2.wav"
    # audio = "/Users/zhaochenguang/Downloads/asr_example.wav"
    result = asr.asr(audio, type="paraformer", language=["zh"])
    print(result)

    