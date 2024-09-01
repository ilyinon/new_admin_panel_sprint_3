from psycopg import connect
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from config import settings
from decorator import backoff
from logger import logger
from models import Movie, GenreFull, Person
from queries import (ACTORS_QUERY, DIRECTORS_QUERY, FILM_WORKS_QUERY,
                     GENRES_QUERY, WRITERS_QUERY, GENRES_LIST_QUERY, PERSONS_LIST_QUERY)


class Postgres:
    def __init__(self):
        self.dsn = make_conninfo(dbname='postgres',
                                 user=settings.POSTGRES_USER,
                                 port=settings.POSTGRES_PORT,
                                 password=settings.POSTGRES_PASSWORD,
                                 host=settings.POSTGRES_HOST,
                                 options='-c search_path=content')

    @backoff()
    def extract(self, entity, last_updated):
        """
        Загрузить данные из postgres новее чем из стейта.
        Вначале получаем данные из film_work.
        После по uuid заполняем actors, directors, writers, genres из
        связанных таблиц
        """
        logger.info("Загрузка данных из индекса %s", entity)
        with connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                if entity == 'film_work':
                    cur.execute(FILM_WORKS_QUERY, {'modified': last_updated})
                    while True:
                        films = cur.fetchmany(settings.BATCH_SIZE)
                        if not films:
                            logger.info("No film to extract")
                            break
                        for film in films:
                            film['actors'] = self.get_detail(film['id'], ACTORS_QUERY)
                            film['directors'] = self.get_detail(film['id'], DIRECTORS_QUERY)
                            film['writers'] = self.get_detail(film['id'], WRITERS_QUERY)
                            film['genres'] = self.get_detail(film['id'], GENRES_QUERY)
                            yield Movie(**film)
                elif entity == 'genres':
                    cur.execute(GENRES_LIST_QUERY, {'modified': last_updated})
                    while True:
                        genres = cur.fetchmany(settings.BATCH_SIZE)
                        if not genres:
                            logger.info("No genres to extract")
                            break
                        for genre in genres:
                            yield GenreFull(**genre) 
                elif entity == 'persons':
                    cur.execute(PERSONS_LIST_QUERY, {'modified': last_updated})
                    while True:
                        persons = cur.fetchmany(settings.BATCH_SIZE)
                        if not persons:
                            logger.info("No persons to extract")
                            break
                        for person in persons:
                            yield Person(**person) 
                else:
                    logger.info("Неизвестный индекс %s", entity)               

    @backoff()
    def get_detail(self, film_id, QUERY):
        """
        Загрузить данные из связанных таблиц по переданному uuid.
        """
        with connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(QUERY, {'film_id': film_id})
                return cur.fetchall()


pg = Postgres()
