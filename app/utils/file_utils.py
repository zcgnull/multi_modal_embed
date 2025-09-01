import os
import json
from io import BytesIO
from PIL import Image
from cachetools import LRUCache, cached
from ruamel.yaml import YAML
from urllib import request

PROJECT_BASE = os.getenv("MME_PROJECT_BASE") or os.getenv("MME_DEPLOY_BASE")

def get_project_base_directory(*args):
    global PROJECT_BASE
    if PROJECT_BASE is None:
        PROJECT_BASE = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                os.pardir,
            )
        )

    if args:
        return os.path.join(PROJECT_BASE, *args)
    return PROJECT_BASE

def load_yaml_conf(conf_path):
    if not os.path.isabs(conf_path):
        conf_path = os.path.join(get_project_base_directory(), conf_path)
    try:
        with open(conf_path) as f:
            yaml = YAML(typ='safe', pure=True)
            return yaml.load(f)
    except Exception as e:
        raise EnvironmentError(
            "loading yaml file config from {} failed:".format(conf_path), e
        )

@cached(cache=LRUCache(maxsize=10))
def load_json_conf(conf_path):
    if os.path.isabs(conf_path):
        json_conf_path = conf_path
    else:
        json_conf_path = os.path.join(get_project_base_directory(), conf_path)
    try:
        with open(json_conf_path) as f:
            return json.load(f)
    except BaseException:
        raise EnvironmentError(
            "loading json file config from '{}' failed!".format(json_conf_path)
        )

def pil_to_fileobj(pil_img: Image.Image) -> BytesIO:
    buf = BytesIO()
    pil_img.convert("RGB").save(buf, format='JPEG')
    buf.seek(0)
    return buf

def save_temp_file(audio):
    """将二进制流或者url转为临时文件，返回文件路径"""
    import tempfile

    if isinstance(audio, bytes):
        # 获取音频格式
        audio_format = get_audio_format_from_header(audio)
        with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{audio_format.lower()}') as temp_file:
            temp_file.write(audio)
            temp_file_path = temp_file.name
        return temp_file_path
    elif isinstance(audio, str) and audio.startswith("http"):
        with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{audio.split(".")[-1]}') as temp_file:
            request.urlretrieve(audio, temp_file.name)
            temp_file_path = temp_file.name
        return temp_file_path
    else:
        raise ValueError("audio must be a bytes stream or an audio URL.")
    
def get_audio_format_from_header(data: bytes) -> str:
    if data.startswith(b'ID3'):
        return 'MP3'
    elif data.startswith(b'RIFF') and b'WAVE' in data[8:12]:
        return 'WAV'
    elif data.startswith(b'fLaC'):
        return 'FLAC'
    elif data[0:4] == b'\xff\xfb' or data[0:4] == b'\xff\xf3' or data[0:4] == b'\xff\xf2':  # MPEG-1 Layer 3
        return 'MP3'
    elif data.startswith(b'\xff\xf1') or data.startswith(b'\xff\xf9'):  # AAC-ADTS
        return 'AAC'
    elif data.startswith(b'ftyp') and (b'm4a' in data[:10] or b'mp4' in data[:10]):
        return 'M4A/AAC'
    else:
        return 'Unknown'