from psycopg import connect, ServerCursor
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row


from logger import logger

from decorator import backoff
from config import settings
from queries import *
from models import Movie
from state import state


class Postgres:
    def __init__(self):
        self.dsn = make_conninfo(dbname='postgres',
                                 user=settings.POSTGRES_USER, 
                                 port=settings.POSTGRES_PORT, 
                                 password=settings.POSTGRES_PASSWORD,
                                 host=settings.POSTGRES_HOST,
                                 options='-c search_path=content')
    
    @backoff()
    def extract(self, last_updated):
        batch_size: int = 100

        with connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(FILM_WORKS_QUERY, {'modified': last_updated})
                while True:
                    films = cur.fetchmany(batch_size)
                    if not films:
                        logger.info("No film to extract")
                        break
                    for film in films:
                        film['actors'] = self.get_detail(film['id'], ACTORS_QUERY)
                        film['directors'] = self.get_detail(film['id'], DIRECTORS_QUERY)
                        film['writers'] = self.get_detail(film['id'], WRITERS_QUERY)
                        film['genres'] = self.get_detail(film['id'], GENRES_QUERY)
                        yield Movie(**film)

    @backoff()
    def get_detail(self, film_id, QUERY):
        with connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(QUERY, {'film_id': film_id})
                return cur.fetchall()

pg = Postgres()