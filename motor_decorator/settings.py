from .controller import MotorDecoratorController
from .tools import db_tools


class MotorDecoratorSettings:
    @classmethod
    def set_extend_logs_state(cls, extended_logs: bool) -> None:
        MotorDecoratorController.EXTENDED_LOGS = extended_logs

    @staticmethod
    def change_log_level(log_level: str) -> None:
        db_tools.LOGGING_LEVEL = log_level
        logger = db_tools.get_logger()
        MotorDecoratorController.logger = logger

    @staticmethod
    def extend_logs_info(turn_on: bool) -> None:
        MotorDecoratorController.EXTENDED_LOGS = turn_on

    @staticmethod
    def ping_clusters() -> None:
        MotorDecoratorController.ping_clusters()
