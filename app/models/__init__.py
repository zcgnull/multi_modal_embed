from .embedding_model import (    
    BaaiVlEmbedding,
    QwenMultiModelEmbed
)

EmbeddingModelFactory = {
    "BAAI": BaaiVlEmbedding,
    "Tongyi-Qianwen": QwenMultiModelEmbed,
}

EmbeddingModel = {
    "BAAI": BaaiVlEmbedding,
    "Tongyi-Qianwen": QwenMultiModelEmbed,
}

TTSModel = {}

ASRModel = {}