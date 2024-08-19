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

        
    def extract(self):
        return pg.extract(self.state_data)

    def transform(self, data):
        tmp = []
        latest = self.state_data
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
                self.load(tmp , latest)
                tmp = []
                latest = self.state_data

        if len(tmp) > 0:
            self.load(tmp , film.modified)

    def load(self, data, modified):
       elastic.load_entry(data, modified, self.state_data)
    
    def get_state(self):
        self.state_str = state.get_state(settings.ELASTIC_INDEX)
        if not self.state_str:
            self.state_data = datetime.now() - timedelta(days=365*1000)
            self.state_str = str(self.state_data)
            state.set_state(settings.ELASTIC_INDEX, str(self.state_str))
        else:
            self.state_data = datetime.strptime(self.state_str, '%Y-%m-%d %H:%M:%S.%f')
        logger.info("the currect state is %s", self.state_str)


if __name__ == '__main__':
    etl = Etl()
    etl.start()
    while True:
        logger.info("Loading data from pg to elastic")
        etl.get_state()
        etl.transform(etl.extract())
        sleep(settings.DELAY_BETWEEN_LOADS)