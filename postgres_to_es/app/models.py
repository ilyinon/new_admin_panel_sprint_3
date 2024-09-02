import uuid
from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel


class MixinClassUUID(BaseModel):
    id: uuid.UUID


class MixinClassModified(MixinClassUUID):
    modified: Optional[Union[datetime, str]]


class PersonRole(MixinClassUUID):
    name: str


class Genre(MixinClassUUID):
    name: str


class Movie(MixinClassModified):
    rating: Optional[float]
    title: str
    description: Optional[str]
    creation_date: Optional[Union[datetime, str]]
    modified: Optional[Union[datetime, str]]
    type: str
    actors: Optional[List[PersonRole]]
    directors: Optional[List[PersonRole]]
    writers: Optional[List[PersonRole]]
    genres: Optional[List[Genre]]


class GenreList(MixinClassModified):
    name: str

class PersonList(MixinClassModified):
    full_name: str
