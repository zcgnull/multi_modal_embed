import logging
from pymilvus import MilvusClient
from typing import Dict, List, Optional, Union

from app.database import settings
from app.database.vector_database import VectorDatabase

logger = logging.getLogger('mme.milvus_database')

ATTEMPT_TIME = 2

class MilvusDatabase(VectorDatabase):
    def __init__(self):
        self.client = MilvusClient(settings.MILVUS['url'])

    def dbType(self) -> str:
        return "milvus"
    
    def health(self):
        health_dict = {}
        health_dict["type"] = "milvus"
        return health_dict
    
    def createCollection(self, collection_name, knowledgebase_id: str, vector_size):
        if self.client.has_collection(collection_name):
            return True
        try:
            return self.client.create_collection(
                collection_name=collection_name,
                dimension=vector_size,
                auto_id=True,
                metric_type="IP",  # Inner product distance
                consistency_level="Bounded",  # Supported values are (`"Strong"`, `"Session"`, `"Bounded"`, `"Eventually"`). See https://milvus.io/docs/consistency.md#Consistency-Level for more details.
            )
        except Exception as e:
            logger.error(f"milvus create collection failed, error: {e}")
        
    
    def deleteCollection(self, collection_name: str, knowledgebase_id: str):
        try:
            self.client.drop_collection(collection_name)
        except Exception as e:
            logger.error(f"milvus delete collection failed, error: {e}")
        
    def collectionExist(self, collection_name: str, knowledgebase_id: str) -> bool:
        try:
            return self.client.has_collection(collection_name)
        except Exception as e:
            logger.error(f"milvus collection exist failed, error: {e}")

    
    def search(self, 
               collection_name: str, 
               data: Union[List[list], list], 
               limit: int = 10, 
               output_fields: Optional[List[str]] = None,
               search_params: Optional[dict] = None):
        
        try:
            search_res = self.client.search(
                collection_name=collection_name,
                data=data,  
                limit=limit,  
                search_params=search_params, 
                output_fields=output_fields, 
            )

            return search_res
        except Exception as e:
            logger.error(f"milvus search failed, error: {e}")
            raise e
        
    def get(self, collection_name, id):
        pass

    def insert(self, collection_name: str, data: Union[List[list], list]):
        res = []
        try:
            res = self.client.insert(
                collection_name=collection_name,
                data=data
            )
        except Exception as e:
            logger.warning(f"milvus insert failed, error: {e}")
        return res
    
    def update(self, collection_name, data):
        pass

    def delete(self, collection_name, filter):
        try:
            res = self.client.delete(
                collection_name=collection_name,
                filter=filter
            )
            return res['delete_count']
        except Exception as e:
            logger.warning(f"milvus delete failed, error: {e}")
    
        return 0

if __name__ == '__main__':
    db = MilvusDatabase()
    result = db.createCollection(collection_name="milvus_test6_d5d682f2", knowledgebase_id='', vector_size=5)
    print(result)
    # data = [
    #     {"id": 0, "vector": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592]},
    #     {"id": 1, "vector": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104]},
    #     {"id": 2, "vector": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592]},
    #     {"id": 3, "vector": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345]},
    #     {"id": 4, "vector": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106]},
    #     {"id": 5, "vector": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955]},
    #     {"id": 6, "vector": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987]},
    #     {"id": 7, "vector": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052]},
    #     {"id": 8, "vector": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336]},
    #     {"id": 9, "vector": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608]}
    # ]
    # data=[
    #     {"id": 0, "vector": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], "color": "pink_8682"},
    #     {"id": 1, "vector": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], "color": "red_7025"},
    #     {"id": 2, "vector": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], "color": "orange_6781"},
    #     {"id": 3, "vector": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345], "color": "pink_9298"},
    #     {"id": 4, "vector": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106], "color": "red_4794"},
    #     {"id": 5, "vector": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955], "color": "yellow_4222"},
    #     {"id": 6, "vector": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987], "color": "red_9392"},
    #     {"id": 7, "vector": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052], "color": "grey_8510"},
    #     {"id": 8, "vector": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336], "color": "white_9381"},
    #     {"id": 9, "vector": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608], "color": "purple_4976"}
    # ]
    # db.insert(collection_name="milvus_test4_d5d682f2", data=data)

    print(db.health())
