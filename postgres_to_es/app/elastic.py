import json

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

from decorator import backoff
from config import settings
from logger import logger



class Elastic:
    def __init__(self):
        self.index = settings.ELASTIC_INDEX
        self.schema = settings.ELASTIC_SCHEMA_PATH
        self.es = Elasticsearch(settings.elastic_url)

    @backoff
    def create_index(self):
        logger.info("Starting")
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
    def load_entry(self, data):
        actions = [ {"_index": self.index, "_source": row, '_id': row['id']} for row in data]
        bulk(self.es, actions)


elastic = Elastic()