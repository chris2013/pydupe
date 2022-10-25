import pytest
from pathlib import Path as p
import tempfile
from pydupe.data import fparms, checkHash, from_path

class TestFparms:
    def test_init(self) -> None:
        assert fparms(hash="1234567890123456789012345678901234567890123456789012345678901234")
        assert fparms(filename="test")
        assert fparms()
        a = fparms(filename=None, hash=None, size=None, inode=None, mtime=None, ctime=None) 
        assert a == fparms()

    def test_checkHash(self) -> None:
        with pytest.raises(ValueError):
            checkHash(fparms(hash="wrong"))
        with pytest.raises(ValueError):
            checkHash(fparms(hash="123456789012345678901234567890123456789012345678901234567890123"))
    
    def test_from_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            somefile = p(tmpdirname) / 'somefile.txt'
            with somefile.open('w') as f:
                f.write('sometext')
            fp = from_path(somefile)
            stat = somefile.stat()
            assert fp.filename == str(somefile)
            assert fp.hash == None
            assert fp.size == stat.st_size
            assert fp.inode == stat.st_ino
            assert fp.mtime == stat.st_mtime
            assert fp.ctime == stat.st_ctime
