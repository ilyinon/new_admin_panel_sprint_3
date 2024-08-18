import logging

from uuid import UUID
from elastic import elastic
from postgres import pg
from time import sleep
from models import Movie

class Etl:
    def start(self):
        elastic.create_index()
    def extract(self):
        return pg.extract()
    def transform(self, data):
        transformed_list = []
        for film in data:
            el = {
                'id': str(film.id),
                'imdb_rating': film.rating,
                'genres': [genre.name for genre in film.genres] if film.genres else '',
                'title': film.title,
                'description': film.description or '',
                'director_names': [director.name for director in film.directors] if film.directors else '',
                'actors_names': [actor.name for actor in film.actors] if film.actors else '',
                'writers_names': [writer.name for writer in film.writers] if film.writers else '',
                'directors': [{'id': str(director.id),
                                 'name': director.name} for director in film.directors] if film.directors else '',
                'actors': [{'id': str(actor.id),
                                 'name': actor.name} for actor in film.actors] if film.actors else '',                                 
                'writers': [{'id': str(writer.id),
                                 'name': writer.name} for writer in film.writers] if film.writers else ''
            }
            tmp = []
            tmp.append(el)
            print(tmp)
            self.load(tmp)
            transformed_list.append(el)
        # print(transformed_list)
        return(transformed_list)

    def load(self, data):
       elastic.load_entry(data)




if __name__ == '__main__':
    etl = Etl()
    etl.start()
    
    etl.transform(etl.extract())
    etl.load(etl.transform(etl.extract()))
    # etl.load()