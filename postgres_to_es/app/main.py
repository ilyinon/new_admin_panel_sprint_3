import logging

from elastic import Elastic
from extractor import PGExtractor
from settings import ETLSettings, PGSettings
from state import JsonFileStorage
from time import sleep



if __name__ == '__main__':
    # ETLProcess().start()s