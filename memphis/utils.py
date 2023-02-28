import random
from threading import Timer
from typing import Callable


class set_interval:
    def __init__(self, func: Callable, sec: int):
        def func_wrapper():
            self.t = Timer(sec, func_wrapper)
            self.t.start()
            func()

        self.t = Timer(sec, func_wrapper)
        self.t.start()

    def cancel(self):
        self.t.cancel()


def default_error_handler(e):
    print("ping exception raised", e)


def get_internal_name(name: str) -> str:
    name = name.lower()
    return name.replace(".", "#")


def random_bytes(amount: int) -> str:
    lst = [random.choice("0123456789abcdef") for n in range(amount)]
    s = "".join(lst)
    return s
