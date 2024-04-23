import asyncio
import functools
import logging
from typing import Callable, Any

from pymongo.errors import DuplicateKeyError, BulkWriteError

from .objects import MotorDecoratorRetryParameters


class MotorDecoratorTools:
    info_format: str = "[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s [%(filename)s/%(funcName)s:%(lineno)d]"
    LOGGING_LEVEL: str = "INFO"
    base_retry_param: MotorDecoratorRetryParameters = MotorDecoratorRetryParameters()

    @staticmethod
    def retry(logger: logging.Logger, init_retries: int = 3, timeout: int = 1) -> Callable:
        def send_request(func: Callable) -> Callable:
            @functools.wraps(func)
            async def wrap(*args, **kwargs) -> Any | None:

                retries = init_retries
                delay = timeout

                retry_param = kwargs.pop("retry_param", MotorDecoratorTools.base_retry_param)

                while retries:
                    try:
                        return await func(*args, **kwargs)
                    except DuplicateKeyError as ex:
                        error_message = (
                            f"DuplicateKeyError (retry={retries % 4}). "
                            f"execution function: <{func.__name__}>, "
                            f"exception description: {ex}"
                        )

                        if retry_param.skip_duplicate_key_error_info is False:
                            logger.error(error_message)
                        return
                    except BulkWriteError as ex:
                        error_message = (
                            f"BulkWriteError (retry={retries % 4}). "
                            f"execution function: <{func.__name__}>, "
                            f"exception description: {ex}"
                        )
                        if error_description := ex.details.get('writeErrors', []):
                            if "E11000" in error_description[0].get("errmsg", ""):
                                if retry_param.skip_duplicate_key_error_info is False:
                                    logger.error(error_message)
                                return

                        logger.error(error_message)
                        retries -= 1
                        await asyncio.sleep(delay)
                        delay *= 2
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
