from playhouse.pool import PooledMySQLDatabase

from app.database.db_models import DB, DataBaseModel
from app.utils import current_timestamp, timestamp_to_date

@DB.connection_context()
def bulk_insert_into_db(model, data_source, replace_on_conflict=False):
    DB.create_tables([model])

    for i, data in enumerate(data_source):
        current_time = current_timestamp() + i
        current_date = timestamp_to_date(current_time)
        if 'create_time' not in data:
            data['create_time'] = current_time
        data['create_date'] = timestamp_to_date(data['create_time'])
        data['update_time'] = current_time
        data['update_date'] = current_date

    preserve = tuple(data_source[0].keys() - {'create_time', 'create_date'})

    batch_size = 1000

    for i in range(0, len(data_source), batch_size):
        with DB.atomic():
            query = model.insert_many(data_source[i:i + batch_size])
            if replace_on_conflict:
                if isinstance(DB, PooledMySQLDatabase):
                    query = query.on_conflict(preserve=preserve)
                else:
                    query = query.on_conflict(conflict_target="id", preserve=preserve)
            query.execute()
