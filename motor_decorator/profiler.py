from .exception import MotorDecoratorClustersNotRegistered
from .objects import MotorDecoratorRegisteredCluster
from .registrator import MotorDecoratorClustersRegistrator
from .settings import MotorDecoratorSettings


def add_cluster(
        cluster_name: str,
        username: str,
        password: str,
        host: str,
        port: int | str,
        response_timeout: int = 10_000,
        **kwargs
) -> None:
    """Adds a new MotorDecoratorCluster to controller"""
    MotorDecoratorProfiler.add_cluster(
        cluster_name=cluster_name,
        username=username,
        password=password,
        host=host,
        port=port,
        response_timeout=response_timeout,
        **kwargs
    )


def profile_clusters(extended_logs: bool = False, log_level: str | None = None, ping: bool = False) -> None:
    """Prepare motor decorator to work"""
    MotorDecoratorProfiler.profile(extended_logs, log_level, ping)


def change_log_level(log_level: str) -> None:
    """Change log level for motor decorator lib"""
    MotorDecoratorProfiler.change_log_level(log_level)


def extend_logs_info(turn_on: bool) -> None:
    """Extend motor decorator logs information"""
    MotorDecoratorProfiler.extend_logs_info(turn_on)


class MotorDecoratorProfiler:
    settings = MotorDecoratorSettings()
    registrator = MotorDecoratorClustersRegistrator()
    clusters: list[MotorDecoratorRegisteredCluster]

    @classmethod
    def add_cluster(
            cls,
            cluster_name: str,
            username: str,
            password: str,
            host: str,
            port: int | str,
            response_timeout: int,
            **kwargs
    ) -> None:
        cls.registrator.registrate(
            cluster_name=cluster_name,
            username=username,
            password=password,
            host=host,
            port=port,
            response_timeout=response_timeout,
            kwargs=kwargs
        )

    @classmethod
    def profile(cls, extended_logs: bool, log_level: str | None, ping: bool) -> None:
        cls._set_extend_logs_state(extended_logs)
        if log_level:
            cls.change_log_level(log_level)
        cls._check_registered_clusters()
        if ping:
            cls._ping_clusters()

    @classmethod
    def _set_extend_logs_state(cls, extended_logs: bool) -> None:
        cls.settings.set_extend_logs_state(extended_logs)

    @classmethod
    def change_log_level(cls, log_level: str) -> None:
        cls.settings.change_log_level(log_level)

    @classmethod
    def extend_logs_info(cls, turn_on: bool) -> None:
        cls.settings.extend_logs_info(turn_on)

    @classmethod
    def _check_registered_clusters(cls) -> None:
        is_not_empty: bool = cls.registrator.clusters_registered()
        if is_not_empty is False:
            raise MotorDecoratorClustersNotRegistered(
                "Clusters not registered for controller. Register clusters in config file from 'add_cluster' method"
            )

    @classmethod
    def _ping_clusters(cls) -> None:
        cls.settings.ping_clusters()
