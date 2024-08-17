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
        for film in data,:
            print(film.keys())
            el = {
                'id': film['id'],
                'title': film['title'],
                'description': film['description'] or '',
                'imdb_rating': film['rating'],
                'genre': [genre['name'] for genre in film['genres']] if film['genres'] else '',
                'director': [director['name'] for director in film['directors']] if film['directors'] else '',
                'actors_names': [actor['name'] for actor in film['actors']] if film['actors'] else '',
                'writers_names': [writer['name'] for writer in film['writers']] if film['writers'] else '',
                'actors': 


            }
            # print(film['id'])
            # el = UUID(film['id'])
            # print(el)
            transformed_list.append(el)
        print(transformed_list)

    def load(self):
       print(elastic.load_entry(pg.extract()))




if __name__ == '__main__':
    etl = Etl()
    etl.start()
    
    etl.transform(etl.extract())
    # etl.load()