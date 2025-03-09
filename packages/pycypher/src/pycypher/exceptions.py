"""Customer exceptions"""


class WrongCypherTypeError(Exception):
    """Exception raised for wrong cypher type"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InvalidCastError(Exception):
    """Exception raised for invalid cast"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
