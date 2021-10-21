import pytest
from pydupe.lutable import LuTable
from collections import deque

class TestLuTable:
    def test_init(self):
        a = LuTable([('key','value')])
        assert isinstance(a,LuTable)
        assert repr(a) == "LuTable([('key', 'value')])"
        assert str(a) == "{'key': deque(['value'])}"
    
    def test_contains(self):
        a = LuTable([(1,2),(3,4)])
        assert (1,2) in a
    
    def test_iter(self):
        a = LuTable([(1,2),(3,4)])
        assert list(a) == [(1,2), (3,4)]

    def test_len(self):
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        assert len(a) == 4
    
    def test_add(self):
        a = LuTable()
        a.add(('key', 'value'))
        assert isinstance(a,LuTable)
        assert repr(a) == "LuTable([('key', 'value')])"
        assert str(a) == "{'key': deque(['value'])}"
    
    def test_discard(self):
        a = LuTable()
        a.add(('key', 'value'))
        a.add((1,2))
        a.remove((1,2))
        assert isinstance(a,LuTable)
        assert repr(a) == "LuTable([('key', 'value')])"
        assert str(a) == "{'key': deque(['value'])}"
    
    def test_lor(self):
        a = LuTable([(1,2),(3,4)])
        b = LuTable([(3,5),(4,6)])
        a.lor(b)
        assert repr(a) == "LuTable([(1, 2), (3, 4), (3, 5)])"

    def test_ldel(self):
        a = LuTable([(1,2),(3,4), (3,5),(4,6),(4,7),(5,8)])
        a.ldel([3,4])
        assert list(a) == [(1,2), (5,8)]
 

    def test_keys(self):
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        assert list(a.keys()) == [1,3]
    
    def test_values(self):
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        assert list(a.values()) == [deque([2]),deque([4,5,6])]

    def test_lexted(self):
        a = LuTable([(1,2),(3,4)])
        b = LuTable([(1,7), (3,5),(4,6)])
        a.lextend(b,3)
        assert repr(a) == "LuTable([(1, 2), (3, 4), (3, 5)])"

    def test_getitem(self):
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        assert a[1] == deque([2])

    def test_setitem(self):
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        a['7'] = ['test']
        assert a['7'] == deque(['test'])
    
    def test_delitem(self):
        a = LuTable([(1,2),(3,4)])
        a['7'] = ['test']
        del a['7']
        assert list(a) == [(1,2), (3,4)]

    def test_as_dict_of_lists_of_str(self):
        a = LuTable([(1,2),(3,4)])
        a['7'] = ['test']
        assert a.as_dict_of_lists_of_str() == {
            1: ['2'],
            3: ['4'],
            '7': ['test']}
