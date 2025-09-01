from app.database.services.commom_service import CommonService
from app.database.db_models import File, DB

class FileService(CommonService):
    model = File

    @classmethod
    @DB.connection_context()
    def get_file_by_id(cls, file_id):
        return None

