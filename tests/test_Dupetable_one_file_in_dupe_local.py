import os
import pathlib
from pathlib import Path as p
import tempfile
import typing as tp

import pydupe.dupetable as dupetable
import pytest
from pydupe.data import fparms
from pydupe.db import PydupeDB

cwd = str(pathlib.Path.cwd())
tdata = cwd + "/pydupe/pydupe/tests/tdata/"
home = str(pathlib.Path.home())


@pytest.fixture
def setup_database() -> tp.Generator[tp.Any, tp.Any, tp.Any]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        dbname = p(newpath + "/.dbtest.sqlite")
        data = [
            fparms(filename= '/tests/tdata/file_exists',
             hash= 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             size= 1,
             inode= 25303464,
             mtime= 1629356592,
             ctime= 1630424506),
            fparms(filename= '/tests/tdata/somedir/file_is_dupe',
             hash= 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             size= 1,
             inode= 25303464,
             mtime= 1629356592,
             ctime= 1630424506)
        ]

        with PydupeDB(dbname) as db:
            db.parms_insert(data)
            db.commit()

        yield

        os.chdir(old_cwd)


@pytest.mark.usefixtures("setup_database")
class TestDupetable_local_scope:
    def test_Dupetable_deletions_local_scope(self) -> None:
        hashlu = dupetable.get_dupes(dbname=p.cwd() / '.dbtest.sqlite')
        deldir = p('/tests/tdata/somedir')
        d, k = dupetable.dd3(hashlu, deldir=deldir, pattern="_dupe",
                             match_deletions=True, dupes_global=False, autoselect=False)
        assert d.as_dict_of_sets() == {}
        assert k.as_dict_of_sets() == {}

