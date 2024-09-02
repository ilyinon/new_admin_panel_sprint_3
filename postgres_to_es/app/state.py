import abc
import json
from datetime import datetime
from typing import Any, Dict

from redis import Redis

from config import settings
from decorator import backoff
from logger import logger


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class RedisStorage(BaseStorage):
    def __init__(self, ):
        self.redis_connection = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)

    @backoff()
    def save_state(self, state: dict) -> None:
        """
        Сохранить стейт в redis.
        """
        self.redis_connection.set('data', json.dumps(state))

    @backoff()
    def retrieve_state(self) -> dict:
        """
        Загрузить стейт из redis.
        """
        raw_data = self.redis_connection.get('data')
        if raw_data is None:
            return {}
        return json.loads(raw_data)


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: RedisStorage) -> None:
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.state[key] = str(value.strftime('%Y-%m-%d %H:%M:%S.%f'))
        print(self.state[key])
        self.storage.save_state(self.state)
        logger.info("state is set to %s", value)

    def get_state(self, key: str) -> datetime:
        """Получить состояние по определённому ключу."""
        current_state = self.state.get(key, None)
        if not current_state:
            return datetime.min
            # initial_date = datetime.min.strftime('%Y-%m-%d %H:%M:%S.%f')
            # return datetime.strptime(initial_date , '%Y-%m-%d %H:%M:%S.%f')
        return datetime.strptime(current_state, '%Y-%m-%d %H:%M:%S.%f')


redis_storage = RedisStorage()

state = State(redis_storage)

