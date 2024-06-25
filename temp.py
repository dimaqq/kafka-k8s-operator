from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from other import Bar


class Foo:
    def __init__(self, arg: Bar):
        self.arg = arg

    
    def get_arg(self) -> Bar:
        return self.arg
