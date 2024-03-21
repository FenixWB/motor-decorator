import asyncio
import functools
import logging
from typing import Callable, Any


class MotorDecoratorTools:
    info_format: str = "[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s [%(filename)s/%(funcName)s:%(lineno)d]"
    LOGGING_LEVEL: str = "INFO"

    @staticmethod
    def retry(logger: logging.Logger, init_retries: int = 3, timeout: int = 1) -> Callable:
        def send_request(func: Callable) -> Callable:
            @functools.wraps(func)
            async def wrap(*args, **kwargs) -> Any | None:

                retries = init_retries
                delay = timeout

                while retries:
                    try:
                        return await func(*args, **kwargs)
                    except Exception as ex:
                        error_message = (
                            f"Database connection error (retry={retries % 4}). "
                            f"execution function: <{func.__name__}>, "
                            f"exception class: <{ex.__class__.__name__}>, "
                            f"exception description: {ex}"
                        )
                        logger.error(error_message)
                        retries -= 1
                        await asyncio.sleep(delay)
                        delay *= 2
                    finally:
                        if retries == 0:
                            error_message = (
                                f"All database retries is finished."
                                f" Check cluster availability."
                            )
                            logger.exception(error_message)
                            return

            return wrap

        return send_request

    def get_logger(self) -> logging.Logger:
        formatter = logging.Formatter(fmt=self.info_format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        logger = logging.getLogger("motor-decorator")
        logger.handlers.clear()

        logger.setLevel(self.LOGGING_LEVEL)
        logger.addHandler(handler)

        return logger


db_tools = MotorDecoratorTools()
