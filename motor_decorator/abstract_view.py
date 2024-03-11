from abc import ABC, abstractmethod
from typing import Self

from pydantic import BaseModel, ConfigDict


class MotorDecoratorAbstractView(ABC, BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @classmethod
    @abstractmethod
    def from_db(cls, data: dict) -> Self:
        raise NotImplementedError

    @classmethod
    def projection(cls) -> dict:
        projection = {field: 1 for field in cls.__annotations__}
        if "id" in projection:
            projection["_id"] = projection.pop("id")
        else:
            projection["_id"] = 0
        return projection
