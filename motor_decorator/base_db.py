from typing import Callable, Any

from .controller import MotorDecoratorController
from .objects import (
    MotorDecoratorClusterName,
    MotorDecoratorDatabaseName,
    MotorDecoratorIndex,
    MotorDecoratorCollectionName
)


def init_collection(collection_name: str, check_existence: bool = False) -> Callable:
    """
    Decorator for init database collection for static databases
     which defined on db class creating
     """

    def internal(func: Callable) -> Callable:
        async def wrap(self, *args, **kwargs) -> Any:
            collection = MotorDecoratorCollectionName(collection_name)
            await self.controller(collection, check_existence)
            result = await func(self, *args, **kwargs)
            return result

        return wrap

    return internal


class MotorDecoratorBaseDB:
    CLUSTER: str
    DATABASE: str

    def __init__(self, test: bool = False) -> None:
        cluster = MotorDecoratorClusterName(self.CLUSTER)
        database = MotorDecoratorDatabaseName(self.DATABASE)
        self.controller = MotorDecoratorController(cluster, database, test)

    async def check_indexes(self, *indexes: MotorDecoratorIndex) -> None:
        await self.controller.check_indexes(*indexes)

    async def init_collection(self, collection_name: str, check_existence: bool = False) -> None:
        collection = MotorDecoratorCollectionName(collection_name)
        await self.controller(collection, check_existence)
