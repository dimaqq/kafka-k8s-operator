from temp import Foo


class Bar:
    def __init__(self):
        return
    
    def build_foo(self) -> Foo:
        return Foo(self)
    
    
