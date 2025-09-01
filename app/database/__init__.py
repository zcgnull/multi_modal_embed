from enum import StrEnum

class TaskStatus(StrEnum):
    UNSTART = "0"
    RUNNING = "1"
    CANCEL = "2"
    DONE = "3"
    FAIL = "4"

class LLMType(StrEnum):
    CHAT = 'chat'
    EMBEDDING = 'embedding'
    ASR = 'asr'
    RERANK = 'rerank'
    TTS    = 'tts'

class FileType(StrEnum):
    DOCUMENT = 'document'
    AUDIO = 'audio'
    VIDEO = 'video'
    IMAGE = 'image'
