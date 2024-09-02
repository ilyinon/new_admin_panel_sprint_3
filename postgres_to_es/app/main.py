from datetime import datetime
from time import sleep

from config import settings
from elastic import elastic
from logger import logger
from postgres import pg
from state import state


class Etl:
    """Основной класс для работы ETL."""

    def start(self):
        """
        Проверить что elastic index существует, или создать новый.
        """
        elastic.create_index()

    def extract(self, entity, entity_state):
        """
        Выгрузить данные из pgsql новее чем из предыдущей загрузки.
        """
        return pg.extract(entity, entity_state)

    def transform(self, data):
        """
        Подготовить данные из формата pgsql в формат elastic.
        """
        tmp = []
        latest = self.state_movies
        for film in data:
            el = {
                'id': str(film.id),
                'imdb_rating': film.rating,
                'genres': [genre.name for genre in film.genres] if film.genres else [],
                'title': film.title.replace('"', '\"'),
                'description': film.description or '',
                'directors_names': [director.name for director in film.directors] if film.directors else [],
                'actors_names': [actor.name for actor in film.actors] if film.actors else [],
                'writers_names': [writer.name for writer in film.writers] if film.writers else [],
                'directors': [{'id': str(director.id),
                               'name': director.name} for director in film.directors] if film.directors else [],
                'actors': [{'id': str(actor.id),
                            'name': actor.name} for actor in film.actors] if film.actors else [],
                'writers': [{'id': str(writer.id),
                             'name': writer.name} for writer in film.writers] if film.writers else []
            }

            tmp.append(el)
            latest = latest if film.modified < latest else film.modified
            if len(tmp) >= settings.BATCH_SIZE:
                self.load(tmp, "movies", latest, self.state_movies)
                tmp = []
                latest = self.state_movies

        if len(tmp) > 0:
            self.load(tmp, "movies", latest, self.state_movies)

    def transform_genres(self, data):
        tmp = []
        latest = self.state_genres
        for genre in data:
            el  = {
                'id': str(genre.id),
                'name': genre.name
            }
            tmp.append(el)
            latest = latest if genre.modified < latest else genre.modified
            if len(tmp) >= settings.BATCH_SIZE:
                self.load(tmp, "genres", latest, self.state_genres)
                tmp = []
                latest = self.state_genres

        if len(tmp) > 0:
            logger.info("genre: %s", tmp)
            self.load(tmp, "genres", latest, self.state_genres)

    def transform_persons(self, data):
        tmp = []
        latest = self.state_persons
        for person in data:
            el  = {
                'id': str(person.id),
                'full_name': person.full_name
            }
            tmp.append(el)
            latest = latest if person.modified < latest else person.modified
            if len(tmp) >= settings.BATCH_SIZE:
                self.load(tmp, "persons", latest, self.state_persons)
                tmp = []
                latest = self.state_persons

        if len(tmp) > 0:
            logger.info("person: %s", tmp)
            self.load(tmp, "persons", latest, self.state_persons)

    def load(self, data, index_name, modified, current_state):
        """
        Загрузить данные в elastic, передаём данные для загрузки,
        наиболее свежую дату модификации из фильмов и текущую дату из стейта
        проверку и обновление стейта делаем после успешной загрузки в elastic.
        """
        elastic.load_entry(data, index_name, modified, current_state)

    def get_state(self):
        """
        Получаем стейт из elastic, если стейта нет делаем фейковый,
        держим стейт в str() и data() чтобы не конвертировать лишний раз.
        """
        self.state_movies = state.get_state("movies")
        self.state_genres = state.get_state("genres")
        self.state_persons = state.get_state("persons")

        logger.info("The currect state movies is %s", self.state_movies)
        logger.info("The currect state genres is %s", self.state_genres)
        logger.info("The currect state persons is %s", self.state_persons)


if __name__ == '__main__':
    etl = Etl()
    etl.start()
    while True:
        logger.info("Loading data from pg to elastic")
        etl.get_state()
        etl.transform(etl.extract("film_work", etl.state_movies))
        etl.transform_genres(etl.extract("genres", etl.state_genres))
        etl.transform_persons(etl.extract("persons", etl.state_persons))
        sleep(settings.DELAY_BETWEEN_LOADS)
