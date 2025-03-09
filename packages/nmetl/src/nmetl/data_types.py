from typing import Any


class DataType:
    def __call__(self, value: Any):
        return self.cast(value)


class _Anything(DataType):
    def cast(self, value: Any):
        return value


class _Integer(DataType):
    def cast(self, value: Any):
        return int(value)


class _PositiveInteger(DataType):
    def cast(self, value: Any):
        return abs(int(float(value)))


class _String(DataType):
    def cast(self, value: Any):
        return str(value)


class _Float(DataType):
    def cast(self, value: Any):
        return float(value)


class _Boolean(DataType):
    def cast(self, value: Any):
        return bool(value)


Anything = _Anything()
Integer = _Integer()
PositiveInteger = _PositiveInteger()
String = _String()
Float = _Float()
Boolean = _Boolean()
