import typing as tp
from collections.abc import Hashable, Iterable
from itertools import chain

K = tp.TypeVar('K', bound=tp.Hashable)
V = tp.TypeVar('V', bound=tp.Hashable)


def check_for_hashable_2tuple(x) -> None:  # type: ignore
    assert isinstance(x, tuple), "no Tuple"
    assert len(x) == 2, "wrong Tuple Size"
    key = x[0]
    value = x[1]
    assert key is not None, "LuTable key cannot be None"
    assert isinstance(key, Hashable), "LuTable tuple: key contains non Hashable"
    assert isinstance(value, Hashable), "LuTable tuple: value contains non Hashable"


def check_argument(x) -> None:  # type: ignore
    if isinstance(x, tuple):
        check_for_hashable_2tuple(x)
    elif isinstance(x, Iterable):
        for t in x:
            check_for_hashable_2tuple(t)
    else:
        raise AssertionError("no Tuple (k,v) of Hashables or Iterable of such Tuples")


class LuTable(tp.Generic[K, V]):
    """
    class used to represent an Lookup Table for File Hashes. This is basically a Dictionary with hashvalue as key and a list of files as values.
    A LuTable is therefore an intermediate between a Mapping and a Set. The set is implemented as list and not as set() objects for performance reasons:
    A set is faster for searching and membership testing, but has a high memory overhead. In contrast lists have low memory overhead but slower membership testing.
    However, the list of files is typically in the order of 10th, therefore memory impact was rated more severe. To speedup adding of elements,
    the list is implemented as collections.deque objects. See for further reference:
    https://docs.python.org/3/library/collections.abc.html
    https://code.activestate.com/recipes/576694/

    A mapping to the hashes is supplied together with set operations for the values. 
    """

    _hashlu: tp.Dict[K, set[V]]

    def __init__(self, x: tp.Union[None, tuple[K, V], list[tuple[K, V]]] = None) -> None:
        self._hashlu = {}
        self._ktype = None
        self._vtype = None
        if x:
            check_argument(x)
            if isinstance(x, list):
                for t in x:
                    if not self._ktype:
                        self._ktype = type(t[0])
                    if not self._vtype:
                        self._vtype = type(t[1])
                    self.add(t)
            else:
                self._ktype = type(x[0])
                self._vtype = type(x[1])
                self.add(x)

    def __ior__(self, other: 'LuTable[K,V]') -> 'LuTable[K,V]':
        assert isinstance(other, LuTable)
        for item in other:
            self.add(item)
        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LuTable):
            raise NotImplementedError
        if self._hashlu == other._hashlu:
            return True
        return False

    def __contains__(self, item: tuple[K, V]) -> bool:
        check_for_hashable_2tuple(item)
        key = item[0]
        value = item[1]
        if key in self._hashlu:
            return (value in self._hashlu[key])
        else:
            return False

    def __iter__(self) -> tp.Iterator[tuple[K, V]]:
        for hash in self._hashlu.keys():
            for file in self._hashlu[hash]:
                yield hash, file

    def __len__(self) -> int:
        return sum([len(x) for x in self._hashlu.values()])

    def add(self, item: tuple[K, V]) -> None:
        check_for_hashable_2tuple(item)
        key = item[0]
        value = item[1]
        if self._ktype:
            assert isinstance(key, self._ktype), "adding inconsistent type"
        if self._vtype:
            assert isinstance(value, self._vtype), "adding inconsistent type"
        if key not in self._hashlu:
            self._hashlu[key] = set()
        self._hashlu[key].add(value)

    def discard(self, x: tuple[K, V]) -> None:
        check_for_hashable_2tuple(x)
        key = x[0]
        value = x[1]
        if key not in self._hashlu.keys():
            raise ValueError("invalid tuple: key wrong")
        if value not in self._hashlu[key]:
            raise ValueError("invalid tuple: value wrong")
        self._hashlu[key].remove(value)
        if self._hashlu[key] == set():
            self._hashlu.pop(key)

    def __str__(self) -> str:
        return(str(self._hashlu))

    def __repr__(self) -> str:
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def lor(self, other: 'LuTable[K,V]') -> None:
        for k, v in iter(other):
            if k in self._hashlu.keys():
                if v not in self._hashlu[k]:
                    self._hashlu[k].add(v)

    def ldel(self, iterable: tp.Optional[tp.Iterable[K]] = None) -> None:
        if iterable is not None:
            for k in iterable:
                del self[k]

    def keys(self) -> tp.KeysView[K]:
        return self._hashlu.keys()

    def values(self) -> tp.ValuesView[set[V]]:
        return self._hashlu.values()

    def chain_values(self) -> tp.Iterable[V]:
        return chain.from_iterable(self.values())

    def lextend(self, other: 'LuTable[K,V]', key: K) -> None:
        if key not in self._hashlu:
            self._hashlu[key] = set()
        for v in other[key]:
            self._hashlu[key].add(v)

    def __getitem__(self, key: K) -> set[V]:
        return self._hashlu[key]

    def __setitem__(self, key: K, value: tp.Iterable[V]) -> None:
        assert type(key) == self._ktype, "setting inconsistent key type"
        for v in value:
            assert type(v) == self._vtype, "setting inconsistent value type"
        self._hashlu[key] = set(value)

    def __delitem__(self, key: K) -> None:
        del self._hashlu[key]

    def as_dict_of_sets(self) -> tp.Dict[str, set[str]]:
        dict_of_sets: tp.Dict[str, set[str]] = {}
        for hash in self._hashlu.keys():
            dict_of_sets[str(hash)] = {str(item) for item in self._hashlu[hash]}
        return dict_of_sets
