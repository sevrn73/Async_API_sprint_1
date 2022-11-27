from functools import wraps
from time import sleep
from logger import logger


def backoff(start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

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
            sleep_time = 0
            result = None

            while sleep_time <= border_sleep_time:
                try:
                    sleep(sleep_time)
                    result = func(*args, *kwargs)
                    break
                except Exception as error:
                    logger.error("Error backoff decorator")
                    if sleep_time == 0:
                        sleep_time = start_sleep_time
                    else:
                        sleep_time = sleep_time * 2**factor if sleep_time < border_sleep_time else border_sleep_time

            return result

        return inner

    return func_wrapper
