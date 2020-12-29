import bz2

def concat(*iterables):
    """Concatenates an arbitrary number of generators/iterables."""
    for it in iterables:
        for e in it:
            yield e

def read_file(filename):
    """
    Like the built-in `open(filename, 'r')`, but can read bz2-compressed
    files as well as plain text. Compressed files must end in '.bz2'.
    """
    bzipped = filename.endswith('.bz2')
    return bz2.open(filename, 'rt') if bzipped else open(filename, 'r')

def multi_file_streamer(*filenames):
    """
    Can open multiple (possibly bz2-compressed) files as though they were one
    single large file.
    """
    return concat(*map(read_file, filenames))

def wrap(f, key='body'):
    """
    Given a function f: A->B that expects to operate on a field of a dict,
    return a function g: dict->dict that mutates a dict using f.

    When building a processing pipeline, some functions need to manipulate
    dictionaries representing comments, and many functions need to specifically
    manipulate a comment's body text, for example. This functions allows
    functions that operate on strings to be turned into functions that take and
    return comments but operate on a field of the dictionary using the original
    function. By default, the 'body' field is used.

    So, for example, `wrap(str.upper)({'body': 'hello'}) == {'body': 'HELLO'}`.
    Here `str.upper` is a function operating on strings, and `wrap(str.upper)`
    is a function operating on comments (using `str.upper`).

    By specifying a key, f can also operate on a different field of the comment.
    Note that the returned function will mutate its input comment in place and
    then return this modified version of the comment as output.
    """
    def g(comment):
        value = comment[key]
        comment[key] = f(value)
        return comment
    return g

def take(n, it):
    """Take an iterator and truncate it to at most n elements."""
    for (count, item) in enumerate(it):
        if count >= n:
            return
        yield item

def take_while(p, it):
    """Take items as long as the predicate p is true."""
    for x in it:
        if p(x):
            yield x
        else:
            return

def drop(n, it):
    """Drop/skip n items."""
    for (count, item) in enumerate(it):
        if count < n:
            continue
        yield item

def drop_while(p, it):
    """Drop items as long as the predicate p is true."""
    for x in it:
        if p(x):
            continue
        else:
            yield x

def foreach(f, it):
    for item in it:
        f(item)

def compose(*funs):
    """
    Compose an arbitrary number of functions. Maybe should be called something
    like "pipe" instead, since functions are used in the order they are given.
    So while mathematical composition would do `(f∘g)(x) = f(g(x))`, this
    instead does `compose(f, g)(x) == g(f(x))` (like unix pipes: `f x | g`).
    """
    def composition(arg):
        tmp = arg
        for f in funs:
            tmp = f(tmp)
        return tmp
    return composition

def flat_map(f, it):
    """
    If f maps an element to an iterator, extract those elements. So flattens
    a stream of streams of elements into a stream of elements.
    For example: `flat_map(range, [1,2,3])` returns
    `[0, 0, 1, 0, 1, 2]` rather than `[[0], [0, 1], [0, 1, 2]]`.
    (Using `[...]` here to represent iterables rather than actual lists.)
    """
    for x in it:
        for y in f(x):
            yield y

def nop(x=None):
    return x

def consumer_to_function(f):
    def g(x):
        f(x)
        return x
    return g

# We're not allowed to use print in lambdas. As a bonus, this is also shorter.
def println(x):
    print(x)

def with_id(f):
    return lambda x: (x, f(x))

def extract_key(key):
    return lambda d: d[key]
