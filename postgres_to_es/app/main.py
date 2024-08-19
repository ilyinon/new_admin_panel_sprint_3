from logger import logger
from elastic import elastic
from postgres import pg
from time import sleep
from state import state
from datetime import datetime, timedelta
from config import settings

class Etl:
    def start(self):
        elastic.create_index()
        self.state = state.get_state(settings.ELASTIC_INDEX)
        if not self.state:
            self.state = str(datetime.now() - timedelta(days=365*1000))
            state.set_state(settings.ELASTIC_INDEX, str(self.state))
        
    def extract(self):
        return pg.extract(datetime.strptime(self.state, '%Y-%m-%d %H:%M:%S.%f'))

    def transform(self, data):
        tmp = []
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
            if len(tmp) >= settings.BATCH_SIZE:
                self.load(tmp , film.modified)
                tmp = []
        if len(tmp) > 0:
            self.load(tmp , film.modified)

    def load(self, data, modified):
       elastic.load_entry(data, modified, datetime.strptime(self.state, '%Y-%m-%d %H:%M:%S.%f'))




if __name__ == '__main__':
    etl = Etl()
    etl.start()
    while True:
        logger.info("Loading data from pg to elastic")
        etl.transform(etl.extract())
        sleep(settings.DELAY_BETWEEN_LOADS)