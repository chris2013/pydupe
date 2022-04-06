import os
import pathlib
import pathlib as pl
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
def setup_database() -> tp.Generator[tp.Any,tp.Any,tp.Any]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        dbname = pl.Path.cwd() / ".dbtest.sqlite"
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
             ctime= 1630424506),
            fparms(filename= '/tests/tdata/somedir/somedir2/file_is_dupe2',
             hash= 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             size= 1,
             inode= 25303464,
             mtime= 1629356592,
             ctime= 1630424506),
            fparms(filename= '/tests/tdata/somedir/dupe_in_dir',
             hash= '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             size= 1,
             inode= 25303464,
             mtime= 1629356592,
             ctime= 1630424506),
            fparms(filename= '/tests/tdata/somedir/dupe2_in_dir',
             hash= '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             size= 1,
             inode= 25303464,
             mtime= 1629356592,
             ctime= 1630424506)]

        with PydupeDB(dbname) as db:
            db.parms_insert(data)
            db.commit()

        yield

        os.chdir(old_cwd)


@pytest.mark.usefixtures("setup_database")
class TestDedupeWithin:
    def test_dedupe_within(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="_dupe", dupes_global=True, match_deletions=True, autoselect=False)
        
        assert deltable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/somedir/file_is_dupe',
            '/tests/tdata/somedir/somedir2/file_is_dupe2'}
        }
        assert keeptable.as_dict_of_sets() == {
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {'/tests/tdata/somedir/dupe2_in_dir', '/tests/tdata/somedir/dupe_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {'/tests/tdata/file_exists'}
        }
    
    
    def test_dedupe_within_match_deletions_False(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        deldir = "/tests/tdata/somedir/"
        _, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="file_", match_deletions=False)
        assert keeptable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/somedir/file_is_dupe',
            '/tests/tdata/somedir/somedir2/file_is_dupe2'}
        }
    
    
    def test_dedupe_within_autoselect_dupes_XXX(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        assert hashlu.as_dict_of_sets() == {'3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {'/tests/tdata/somedir/dupe2_in_dir', '/tests/tdata/somedir/dupe_in_dir'}, 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {'/tests/tdata/file_exists', '/tests/tdata/somedir/file_is_dupe', '/tests/tdata/somedir/somedir2/file_is_dupe2'}}
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="_dupe", autoselect=True, dupes_global=False, match_deletions=True)
    
        assert deltable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/somedir/file_is_dupe'}
        }
    
        # assert keeptable.as_dict_of_lists_of_str() == {
        #     '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': ['/tests/tdata/somedir/dupe2_in_dir', '/tests/tdata/somedir/dupe_in_dir'],
        #     'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': ['/tests/tdata/somedir/somedir2/file_is_dupe2']}
    
    
    def test_dedupe_within_dupes_global_on_match_deletions(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        deldir = "/tests/tdata/somedir/somedir2"
        deltable, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="file_", dupes_global=True, match_deletions=True)
        assert deltable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/somedir/somedir2/file_is_dupe2'}
        }
        assert keeptable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/file_exists',
            '/tests/tdata/somedir/file_is_dupe'}
        }
    
    
    def test_dedupe_within_dupes_global_on_match_keeps(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        deldir = "/tests/tdata/somedir/somedir2"
        deltable, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="file_", dupes_global=True, match_deletions=False, autoselect=False)
        assert keeptable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/somedir/somedir2/file_is_dupe2'}
        }
        assert deltable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/file_exists',
            '/tests/tdata/somedir/file_is_dupe'}
        }
    
    
    def test_dedupe_within_dupes_global_off_autoselect_off(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="file_", dupes_global = False, match_deletions = True, autoselect = False)
        assert deltable.as_dict_of_sets() == {}
        assert keeptable.as_dict_of_sets() == {
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {'/tests/tdata/somedir/dupe2_in_dir', '/tests/tdata/somedir/dupe_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {'/tests/tdata/somedir/file_is_dupe', '/tests/tdata/somedir/somedir2/file_is_dupe2'}
        }
    
    
    def test_dedupe_within_dupes_global_off_autoselect_on(self) -> None:
        hashlu = dupetable.get_dupes(dbname = pl.Path.cwd() / '.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = dupetable.dd3(hashlu, deldir=deldir, pattern="file_", dupes_global = False, match_deletions = True, autoselect = True)
        assert deltable.as_dict_of_sets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
            {'/tests/tdata/somedir/file_is_dupe'}
        }
        assert keeptable.as_dict_of_sets() == {
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {'/tests/tdata/somedir/dupe2_in_dir', '/tests/tdata/somedir/dupe_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {'/tests/tdata/somedir/somedir2/file_is_dupe2'}
        }
