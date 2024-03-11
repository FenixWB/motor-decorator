from .controller import MotorDecoratorController
from .objects import MotorDecoratorClusterName, MotorDecoratorRegisteredCluster, MotorDecoratorClusterUrl

__all__ = ["MotorDecoratorClustersRegistrator", ]


class MotorDecoratorClustersRegistrator:
    """
    MotorDecoratorClustersRegistrator

    This class is responsible for registering MotorDecoratorClusters. It provides a method to register
     a cluster and a private method to wrap the registration process.

    Methods:
    - `registrate(
        cluster_name: str,
         username: str,
          password: str,
           host: str,
            port: int,
             response_timeout: int,
              kwargs: dict
              ) -> None`: Registers a cluster by calling the _wrap_registration
    * method and adding the cluster to the MotorDecoratorController.

    Private Methods:
    - `_wrap_registration(
        cluster_name: str,
         username: str,
          password: str,
           host: str,
            port: int,
             response_timeout: int,
              kwargs: dict
              ) -> MotorDecoratorRegisteredCluster`: Wraps the registration
    * process by creating a MotorDecoratorRegisteredCluster object with the provided parameters.

    """

    def registrate(
            self,
            cluster_name: str,
            username: str,
            password: str,
            host: str,
            port: int | str,
            response_timeout: int,
            kwargs: dict
    ) -> None:
        cluster = self._wrap_registration(
            cluster_name=cluster_name,
            username=username,
            password=password,
            host=host,
            port=port,
            response_timeout=response_timeout,
            kwargs=kwargs
        )
        MotorDecoratorController.add_cluster(cluster)

    @staticmethod
    def _wrap_registration(
            cluster_name: str,
            username: str,
            password: str,
            host: str,
            port: int | str,
            response_timeout: int,
            kwargs: dict
    ) -> MotorDecoratorRegisteredCluster:
        return MotorDecoratorRegisteredCluster(
            cluster_name=MotorDecoratorClusterName(cluster_name),
            cluster_url=MotorDecoratorClusterUrl(
                username=username,
                password=password,
                host=host,
                port=port
            ),
            response_timeout=response_timeout,
            **kwargs
        )

    @staticmethod
    def clusters_registered() -> bool:
        registered_clusters = MotorDecoratorController.clusters
        if registered_clusters:
            return True
        return False
