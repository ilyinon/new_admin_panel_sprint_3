import uuid

from datetime import datetime
from typing import List, Optional
from typing import Union

from pydantic import BaseModel


class PersonRole(BaseModel):
    id: uuid.UUID
    name: str


class Genre(BaseModel):
    id: uuid.UUID
    name: str


class Movie(BaseModel):
    id: uuid.UUID
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
