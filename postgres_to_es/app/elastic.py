import json

from elasticsearch import Elasticsearch, NotFoundError

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
        try:
            self.es.indices.get(index=self.index)
            logger.info("%s is exist", self.index)
        except NotFoundError:
            logger.info("%s is not exist", self.index)
            with open(self.schema, 'r') as _:
                elastic_schema_json = json.load(_)
                logger.info("%s is opened", self.schema)
            logger.info("Trying to create %s schema in ES", self.schema)
            self.es.indices.create(index=self.index, body=elastic_schema_json)
            logger.info("%s schema is created in ES", self.schema)

logger.info("Starting")
elastic = Elastic()
elastic.create_index()