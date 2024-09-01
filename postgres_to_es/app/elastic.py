import json

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

from config import settings
from decorator import backoff
from logger import logger
from state import state


class Elastic:
    def __init__(self):
        self.index = settings.ELASTIC_INDEX
        self.indexes = settings.ELASTIC_INDEXES.split(',')
        self.schema = settings.ELASTIC_SCHEMA_PATH
        self.schema_folder = settings.ELASTIC_SCHEMA_FOLDER
        self.es = Elasticsearch(settings.elastic_url)
        print(self.es)

    @backoff()
    def create_index(self):
        """
        Проверить что Elastic index существует, или создать новый.
        """
        logger.info("ES is initializing")
        for index in self.indexes:
            try:
                self.es.indices.get(index=index)
            except NotFoundError:
                logger.info("%s is not exist", index)
                path_to_schema = f"{self.schema_folder}/{index}.json"
                with open(path_to_schema, 'r') as _:
                    elastic_schema_json = json.load(_)
                    logger.info("%s is opened", path_to_schema)
                logger.info("Trying to create %s schema in ES", path_to_schema)
                self.es.indices.create(index=index, body=elastic_schema_json)
                logger.info("%s schema is created in ES", path_to_schema)
            finally:
                logger.info("%s index is ready", index)

    @backoff()
    def load_entry(self, data, index_name, modified, last_updated):
        """
        Загрузить данные в elastic пачкой, обновить стейт при успехе.
        """
        film_to_upload = [{"_index": self.index, "_source": row, '_id': row['id']} for row in data]
        success, failed = bulk(self.es, film_to_upload)

        if success:
            the_recent_state = last_updated if modified < last_updated else modified
            state.set_state(index_name, the_recent_state)
            logger.info("Uploaded to ES successfully: %s, %s", len(data), the_recent_state)
        
        if len(failed) > 0:
            logger.info("Uploading to ES is failed: %s", len(failed))
            for failed_items in failed:
                logger.error(failed_items)


elastic = Elastic()
