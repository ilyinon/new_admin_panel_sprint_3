import logging
import time
from functools import wraps

import elasticsearch
import psycopg
import redis

from logger import logger


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=30):
    """
        Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
         Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

        Формула:
            t = start_sleep_time * 2^(n) if t < border_sleep_time
            t = border_sleep_time if t >= border_sleep_time
        :param start_sleep_time: начальное время повтора
        :param factor: во сколько раз нужно увеличить время ожидания
        :param border_sleep_time: граничное время ожидания
        :return: результат выполнения функции
        """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while sleep_time < border_sleep_time:
                try:
                    return func(*args, **kwargs)

                except (redis.ConnectionError, 
                        psycopg.errors.ConnectionTimeout,
                        elasticsearch.ConnectionError) as error:
                    logger.error(error)

                time.sleep(sleep_time)
                sleep_time = (sleep_time + factor) * 2

        return inner

    return func_wrapper
