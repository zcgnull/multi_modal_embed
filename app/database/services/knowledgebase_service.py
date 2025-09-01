from app.database.services.commom_service import CommonService
from app.database.db_models import Knowledgebase, DB

class KnowledgebaseService(CommonService):
    model = Knowledgebase

    @classmethod
    @DB.connection_context()
    def get_by_name(cls, kb_name):
        kbs = cls.model.select().where(cls.model.name == kb_name).paginate(0, 1)
        kbs = kbs.dicts()
        return list(kbs)
    


if __name__ == "__main__":
    
    kbs = KnowledgebaseService.get_or_none(name="test3")
    print(kbs)