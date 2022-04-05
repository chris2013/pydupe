import pytest
from pydupe.lutable import LuTable
import pathlib as pl

class TestLuTable:
    value = pl.Path("/tmp")
    value2 = pl.Path("/tmp2")
    two = pl.Path("/tmp/two")
    three = pl.Path("/tmp/three")
    four = pl.Path("/tmp/four")
    five = pl.Path("/tmp/five")
    six = pl.Path("/tmp/six")
    seven = pl.Path("/tmp/seven")
    eight = pl.Path("/tmp/eight")

    @pytest.mark.parametrize('tpl',
    [1,      # wrong or no tuple
    (1,2,3),
    [(1,2),3],
    [(1,2), (1,2,3)],
    ([1],2), # no Hashable
    (1,[2]),
    (None,1)  
    ])
    def test_for_invalid_argument(self, tpl) -> None: # type: ignore
        with pytest.raises(AssertionError):
            a = LuTable(tpl) # type: ignore
        a = LuTable()
        with pytest.raises(AssertionError):
            a.add(tpl)

    def test_init(self) -> None:
        a = LuTable(('key',self.value))
        assert isinstance(a,LuTable)
        # assert repr(a) == "LuTable([('key', value)])"
        # assert str(a) == "{'key': {value}}"
    
    def test_contains(self) -> None:
        a = LuTable([("1",self.two),("3",self.four)])
        assert ("1",self.two) in a
    
    def test_iter(self) -> None:
        a = LuTable([("1",self.two),("3",self.four)])
        b = [i for i in a]
        assert list(a) == b == [("1",self.two), ("3",self.four)]

    def test_len(self) -> None:
        a = LuTable([('1',self.two),('3',self.four),('3',self.five),('3',self.six)])
        assert len(a) == 4
    
    def test_add(self) -> None:
        a = LuTable[str,pl.Path]()
        a.add(('key', self.value))
        assert isinstance(a,LuTable)
        assert a == LuTable([('key', self.value)])
        
        a = LuTable(('key1',self.value))
        a.add(('key2', self.value2))
        assert isinstance(a,LuTable)
        assert a == LuTable([('key1', self.value), ('key2', self.value2)])

        wrongtype = LuTable((3,4))
        wrongtype |= LuTable((5,6))
        with pytest.raises(AssertionError):
            wrongtype |= a # type: ignore
        with pytest.raises(AssertionError):
            LuTable([(5,'6'), (7,8)])

    def test_discard(self) -> None:
        a = LuTable((1,2))
        a.add((3,4))
        with pytest.raises(AssertionError):
            a.discard(2) #type: ignore
        with pytest.raises(AssertionError):
            a.discard((1,2,3)) #type: ignore
        with pytest.raises(ValueError):
            a.discard((5,5))
        with pytest.raises(ValueError):
            a.discard((1,3))

        a.discard((1,2))
        assert a == LuTable((3, 4))
    
    def test_lor(self) -> None:
        a = LuTable([('1',self.two),('3',self.four)])
        b = LuTable([('3',self.five),('4',self.six)])
        a.lor(b)
        assert a == LuTable([('1',self.two),('3',self.five),('3',self.four)])

    def test_ior(self) -> None: 
        a = LuTable([('1',self.two),('3',self.four)])
        b = LuTable([('3',self.five),('4',self.six)])
        a|=b
        assert a == LuTable([('1', self.two), ('3', self.four), ('3', self.five), ('4', self.six)])

    def test_ldel(self) -> None:
        a = LuTable([('1',self.two),('3',self.four), ('3',self.five),('4',self.six),('4',self.seven),('5',self.eight)])
        a.ldel(['3','4'])
        assert a == LuTable([('1',self.two), ('5',self.eight)])
        a.ldel(['1'])
        assert a == LuTable(('5',self.eight))

    def test_keys(self) -> None:
        a = LuTable([('1',self.two),('3',self.four),('3',self.five),('3',self.six)])
        assert list(a.keys()) == ['1','3']
    
    def test_values(self) -> None:
        a = LuTable([('1',self.two),('3',self.four),('3',self.five),('3',self.six)])
        assert list(a.values()) == [{self.two}, {self.four,self.five,self.six}]

    def test_chain_values(self) -> None:
        a = LuTable([('1',self.two),('3',self.four),('3',self.five),('3',self.six)])
        assert set(a.chain_values()) == {self.two,self.four,self.five,self.six}

    def test_lexted(self) -> None:
        a = LuTable([(1,2),(3,4)])
        b = LuTable([(1,7), (3,5),(4,6)])
        a.lextend(b,3)
        assert repr(a) == "LuTable([(1, 2), (3, 4), (3, 5)])"

    def test_getitem(self) -> None:
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        assert a[1] == {2}

    def test_setitem(self) -> None:
        a = LuTable([(1,2),(3,4),(3,5),(3,6)])
        a[7] = [8]
        assert a[7] == {8}
        with pytest.raises(AssertionError):
            a[7] = [8,'9'] # type: ignore  

    def test_delitem(self) -> None:
        a = LuTable([(1,2),(3,4)])
        a[7] = [8]
        del a[7]
        assert a == LuTable([(1,2), (3,4)])

    def test_as_dict_of_sets(self) -> None:
        a = LuTable([(1,2),(3,4)])
        a[7] = [8, 9]
        assert a.as_dict_of_sets() == {
            '1': {'2'},
            '3': {'4'},
            '7': {'8','9'}
            }
