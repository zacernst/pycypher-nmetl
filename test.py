class Foo:
    def __init__(self, x, y):
        self.x = x
        self.y = y


f = Foo(1, 2)

match f:
    case Foo():
        print("hi")
    case _:
        print("bye")
