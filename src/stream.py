from util import *

class Stream():
    """
    General purpose (minimal) stream implementation.
    Mostly imitating java.util.stream.Stream.
    """
    def __init__(self, iterable):
        self.base = iter(iterable)

    def map(self, f):
        return Stream(map(f, self.base))

    def flat_map(self, f):
        return Stream(flat_map(f, self.base))

    def filter(self, f):
        return Stream(filter(f, self.base))

    def peek(self, f):
        return self.map(consumer_to_function(f))

    def foreach(self, f):
        for e in self.base: f(e)

    def concat(self, s):
        return Stream(concat(self, s))

    def take(self, n):
        return Stream(take(n, self))

    def take_while(self, p):
        return Stream(take_while(p, self))

    def drop(self, n):
        return Stream(drop(n, self))

    def drop_while(self, p):
        return Stream(drop_while(p, self))

    def distinct(self):
        def f():
            seen = set()
            for x in self.base:
                if x not in seen:
                    seen.add(x)
                    yield x
        return Stream(f())

    def to_list(self):
        return list(self)

    def to_string(self, sep=''):
        return sep.join(map(str, self.base))

    def to_dict(self, kf=lambda x: x, vf=lambda x: x, merge=None):
        """
        kf determines the key, vf determines the value, merge is used
        to update an existing mapping. In other words, when a mapping
        with that key already exists, the new value is merge(old, new).
        All are optional. The default for kf and vf is the identity
        function, and the default for collisions is to overwrite.
        """
        d = {}
        for x in self.base:
            key = kf(x)
            val = vf(x)
            if merge is None or key not in d:
                d[key] = val
            else:
                d[key] = merge(d[key], val)
        return d

    def count(self):
        n = 0
        for _ in self.base:
            n += 1
        return n

    def reduce(self, op, seed=None):
        result = seed if seed is not None else next(self)
        for x in self:
            result = op(result, x)
        return result

    def __next__(self):
        return next(self.base)

    def __iter__(self):
        return self
