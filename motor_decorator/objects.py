from .exception import MotorDecoratorValueError, MotorDecoratorTypeError


class MotorDecoratorNameObject:
    def __init__(self, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError(f"Name must be a string, not a {type(name)}!")
        elif not name:
            raise ValueError(f"Name must not be not empty string!")
        self.name = name

    def __eq__(self, other) -> bool:
        if isinstance(other, str):
            return self.name == other
        elif isinstance(other, MotorDecoratorNameObject):
            return self.name == other.name
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


class MotorDecoratorClusterName(MotorDecoratorNameObject):
    """Class to define mongo cluster name"""
    pass


class MotorDecoratorDatabaseName(MotorDecoratorNameObject):
    """Class to define mongo database name in cluster"""
    pass


class MotorDecoratorCollectionName(MotorDecoratorNameObject):
    """Class to define mongo collection name in database"""
    pass


class MotorDecoratorIndex:
    def __init__(self, *name: str, unique: bool = False, **kwargs) -> None:
        self.name = name
        self.unique = unique
        self.kwargs = kwargs

    def __eq__(self, other: str) -> bool:
        if isinstance(other, str):
            return self.name == other
        return self == other

    def __hash__(self) -> int:
        return hash(self.name)


class MotorDecoratorClusterUrl:
    url_template = "mongodb://{username}:{password}@{host}:{port}/"

    def __init__(self, username: str, password: str, host: str, port: int | str) -> None:
        self._validate_arguments(username, password, host, port)
        self.url = self.url_template.format(username=username, password=password, host=host, port=port)

    @staticmethod
    def _validate_arguments(username: str, password: str, host: str, port: int) -> None:
        if not isinstance(username, str):
            raise MotorDecoratorTypeError(f"Username type must be string, not a '{type(username)}'!")
        elif not username:
            raise MotorDecoratorValueError(f"Username must be non-empty string!")

        if not isinstance(password, str):
            raise MotorDecoratorTypeError(f"Password type must be string, not a '{type(password)}'!")
        elif not password:
            raise MotorDecoratorValueError(f"Password must be must be non-empty string!")

        if not isinstance(host, str):
            raise MotorDecoratorTypeError(f"Host type must be string, not a '{type(host)}'!")
        elif not host:
            raise MotorDecoratorValueError(f"Host must be non-empty string!")

        if not isinstance(port, int) and not isinstance(port, str):
            raise MotorDecoratorTypeError(f"Port type must be <int> or valid <str> which can convert to int,"
                                          f" not a '{type(port)}'!")
        elif isinstance(port, int):
            if port < 1 or port > 65535:
                raise MotorDecoratorValueError(f"Port must be int between 1 to 65535")
        elif isinstance(port, str):
            port = int(port)
            if port < 1 or port > 65535:
                raise MotorDecoratorValueError(f"Port must be int between 1 to 65535")

    def __repr__(self) -> str:
        attributes_string = tuple(f"{attr}={value}" for attr, value in self.__dict__.items())
        return f"{self.__class__.__name__}({', '.join(attributes_string)})"


class MotorDecoratorRegisteredCluster:
    def __init__(
            self,
            cluster_name: MotorDecoratorClusterName,
            cluster_url: MotorDecoratorClusterUrl,
            response_timeout: int,
            **kwargs
    ) -> None:
        self.name = cluster_name.name
        self.url = cluster_url.url
        self.timeout = response_timeout
        self.kwargs = kwargs

    def __repr__(self) -> str:
        attributes_string = tuple(f"{attr}={value}" for attr, value in self.__dict__.items())
        return f"{self.__class__.__name__}({', '.join(attributes_string)})"
