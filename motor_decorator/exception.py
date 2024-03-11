class MotorDecoratorCollectionNotFoundError(Exception):
    """If database not contain required collection"""


class MotorDecoratorClustersNotRegistered(Exception):
    """If clusters not added to controller"""


class MotorDecoratorViewError(Exception):
    """If received wrong type of subclass of AbstractView"""


class MotorDecoratorValueError(ValueError):
    ...


class MotorDecoratorTypeError(TypeError):
    ...
