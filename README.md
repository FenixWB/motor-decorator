# motor-decorator
This project be a decorator for motor lib. Structure and hide low code realisation to database access

Install:

```python
pip
install
motor - decorator
```

Create fast database class using a motor-decorator:

```python
from typing import Self
from motor_decorator import MotorDecoratorBaseDB, MotorDecoratorAbstractView, init_collection
from pydantic import Field
from bson import ObjectId


class VendorDatabaseView(MotorDecoratorAbstractView):
    id: ObjectId
    supplier_id: int
    api_key: str
    members: list[int] = Field(default_factory=list)

    @classmethod
    def from_db(cls, data: dict) -> Self:
        data["id"] = data.pop("_id")
        return cls(**data)


class VendorDB(MotorDecoratorBaseDB):
    CLUSTER = "MAIN"
    DATABASE = "F_VENDOR"
    VENDORS = "VENDORS"

    async def get_vendors(self) -> list[VendorDatabaseView]:
        await self.init_collection(self.VENDORS, check_existence=True)
        condition = {
            "KEY_NEW": {"$ne": None},
            "NEW_KEY_DISABLED": False,
            "SUPPLIER_ID": {
                "$ne": 0,
            }
        }
        projection = VendorDatabaseView.projection()
        vendors = await self.controller.do_find_many(condition, projection, VendorDatabaseView)
        return vendors

    # or
    @init_collection(VENDORS, check_existence=True)
    async def get_vendors(self) -> list[VendorDatabaseView]:
        condition = {
            "KEY_NEW": {"$ne": None},
            "NEW_KEY_DISABLED": False,
            "SUPPLIER_ID": {
                "$ne": 0,
            }
        }
        projection = VendorDatabaseView.projection()
        vendors = await self.controller.do_find_many(condition, projection, VendorDatabaseView)
        return vendors
```

Create a MongoDB clusters set and easily bind your database to the cluster:

```python
# config.py
import os
from motor_decorator import add_cluster, profile_clusters

MONGO_HOST_MAIN = os.getenv("MONGO_HOST_MAIN")
MONGO_PORT_MAIN = os.getenv("MONGO_PORT_MAIN")
MONGO_USER_MAIN = os.getenv("MONGO_USER_MAIN")
MONGO_PASSWORD_MAIN = os.getenv("MONGO_PASSWORD_MAIN")

MONGO_HOST_LOCAL = os.getenv("MONGO_HOST_LOCAL")
MONGO_PORT_LOCAL = os.getenv("MONGO_PORT_LOCAL")
MONGO_USER_LOCAL = os.getenv("MONGO_USER_LOCAL")
MONGO_PASSWORD_LOCAL = os.getenv("MONGO_PASSWORD_LOCAL")


def init_clusters():
    add_cluster(
        cluster_name="MAIN",
        username=MONGO_USER_MAIN,
        password=MONGO_PASSWORD_MAIN,
        host=MONGO_HOST_MAIN,
        port=MONGO_PORT_MAIN,
        response_timeout=10_000
    )
    add_cluster(
        cluster_name="LOCAL",
        username=MONGO_USER_LOCAL,
        password=MONGO_PASSWORD_LOCAL,
        host=MONGO_HOST_LOCAL,
        port=MONGO_PORT_LOCAL,
        response_timeout=5_000
    )
    profile_clusters(extended_logs=True, log_level="DEBUG", ping=True)


init_clusters()

# or

import os
from motor_decorator import add_cluster, profile_clusters

MONGO_HOST_MAIN = os.getenv("MONGO_HOST_MAIN")
MONGO_PORT_MAIN = os.getenv("MONGO_PORT_MAIN")
MONGO_USER_MAIN = os.getenv("MONGO_USER_MAIN")
MONGO_PASSWORD_MAIN = os.getenv("MONGO_PASSWORD_MAIN")

MONGO_HOST_LOCAL = os.getenv("MONGO_HOST_LOCAL")
MONGO_PORT_LOCAL = os.getenv("MONGO_PORT_LOCAL")
MONGO_USER_LOCAL = os.getenv("MONGO_USER_LOCAL")
MONGO_PASSWORD_LOCAL = os.getenv("MONGO_PASSWORD_LOCAL")

add_cluster(
    cluster_name="MAIN",
    username=MONGO_USER_MAIN,
    password=MONGO_PASSWORD_MAIN,
    host=MONGO_HOST_MAIN,
    port=MONGO_PORT_MAIN,
    response_timeout=10_000
)
add_cluster(
    cluster_name="LOCAL",
    username=MONGO_USER_LOCAL,
    password=MONGO_PASSWORD_LOCAL,
    host=MONGO_HOST_LOCAL,
    port=MONGO_PORT_LOCAL,
    response_timeout=5_000
)
profile_clusters(extended_logs=True, log_level="DEBUG", ping=True)


```