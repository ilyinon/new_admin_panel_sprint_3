import logger

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
        # self.state = datetime.strptime(state.get_state(settings.ELASTIC_INDEX), '%Y-%m-%d %H:%M:%S.%f')

        print(self.state)
        if not self.state:
            self.state = str(datetime.now() - timedelta(days=365*1000))
            # a= (str(self.state))
            # print(datetime.strptime(a, '%Y-%m-%d %H:%M:%S.%f'))
            ### self.state = datetime.strptime(self.state, '%Y-%m-%d %H:%M:%S.%f')
            state.set_state(settings.ELASTIC_INDEX, str(self.state))
        
    def extract(self):
        return pg.extract(datetime.strptime(self.state, '%Y-%m-%d %H:%M:%S.%f'))

    def transform(self, data):
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
            tmp = []
            tmp.append(el)
            self.load(tmp , film.modified)

    def load(self, data, modified):
       elastic.load_entry(data, modified, datetime.strptime(self.state, '%Y-%m-%d %H:%M:%S.%f'))




if __name__ == '__main__':
    etl = Etl()
    etl.start()
    
    etl.transform(etl.extract())
    # etl.load(etl.transform(etl.extract()))
    # etl.load()