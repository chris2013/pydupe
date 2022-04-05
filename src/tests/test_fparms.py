import pytest
from pydupe.data import fparms

class TestFparms:
    def test_init(self) -> None:
        # good
        assert fparms(hash="1234567890123456789012345678901234567890123456789012345678901234")
        assert fparms(filename="test")
        assert fparms()
        a = fparms(filename=None, hash=None, size=None, inode=None, mtime=None, ctime=None) 
        assert a == fparms()
        # bad
        with pytest.raises(ValueError):
            fparms(hash="wrong")
        with pytest.raises(ValueError):
            fparms(hash="123456789012345678901234567890123456789012345678901234567890123")
