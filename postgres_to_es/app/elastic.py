import json

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

from decorator import backoff
from config import settings
from logger import logger
from state import state





class Elastic:
    def __init__(self):
        self.index = settings.ELASTIC_INDEX
        self.schema = settings.ELASTIC_SCHEMA_PATH
        self.es = Elasticsearch(settings.elastic_url)

    @backoff
    def create_index(self):
        try:
            self.es.indices.get(index=self.index)
        except NotFoundError:
            logger.info("%s is not exist", self.index)
            with open(self.schema, 'r') as _:
                elastic_schema_json = json.load(_)
                logger.info("%s is opened", self.schema)
            logger.info("Trying to create %s schema in ES", self.schema)
            self.es.indices.create(index=self.index, body=elastic_schema_json)
            logger.info("%s schema is created in ES", self.schema)
        finally:
            logger.info("%s is exist", self.index)
    def load_entry(self, data, modified, last_updated):
        if modified > last_updated:
            last_updated = modified
        film_to_upload = []
        for row in data:
            film_to_upload.append({"_index": self.index, "_source": row, '_id': row['id']})
        film_to_upload = [ {"_index": self.index, "_source": row, '_id': row['id']} for row in data]
        success, _ = bulk(self.es, film_to_upload)

        if success:
            state.set_state(settings.ELASTIC_INDEX, str(last_updated))
            logger.info("Uploaded to ES successfully")


elastic = Elastic()