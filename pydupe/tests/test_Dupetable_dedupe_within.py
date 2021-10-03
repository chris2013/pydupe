import os
import pathlib
import tempfile

import pytest
from pydupe.dupetable import Dupetable
from pydupe.db import PydupeDB

cwd = str(pathlib.Path.cwd())
tdata = cwd + "/pydupe/pydupe/tests/tdata/"
home = str(pathlib.Path.home())

@pytest.fixture
def setup_database():
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        dbname = newpath + "/.dbtest.sqlite"
        data = [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/somedir2/file_is_dupe2',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

        with PydupeDB(dbname) as db:
            for d in data:
                ftuple = (d['filename'], d['hash'], d['size'],
                          d['inode'], d['mtime'], d['ctime'])
                db.insert(ftuple)
            db.commit()

        yield

        os.chdir(old_cwd)


@pytest.mark.usefixtures("setup_database")
class TestDedupeWithin:
    def test_dedupe_within(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = ds.dd3(
            deldir, "_dupe", dupes_global=True, match_deletions=True, autoselect=False)
        assert deltable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/file_is_dupe',
            '/tests/tdata/somedir/somedir2/file_is_dupe2']
        }
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/file_exists']
        }
    
    
    def test_dedupe_within_match_deletions_False(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir/"
        deltable, keeptable = ds.dd3(
            deldir, "file_", match_deletions=False)
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/file_is_dupe',
            '/tests/tdata/somedir/somedir2/file_is_dupe2']
        }
    
    
    def test_dedupe_within_autoselect_dupes(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = ds.dd3(
            deldir, "_dupe", autoselect=True, dupes_global=False, match_deletions=True)
    
        assert deltable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/file_is_dupe']
        }
    
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/somedir2/file_is_dupe2'],
        }
    
    
    def test_dedupe_within_dupes_global_on_match_deletions(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir/somedir2"
        deltable, keeptable = ds.dd3(
            deldir, "file_", dupes_global=True, match_deletions=True)
        assert deltable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/somedir2/file_is_dupe2']
        }
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/file_exists',
            '/tests/tdata/somedir/file_is_dupe']
        }
    
    
    def test_dedupe_within_dupes_global_on_match_keeps(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir/somedir2"
        deltable, keeptable = ds.dd3(
            deldir, "file_", dupes_global=True, match_deletions=False, autoselect=False)
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/somedir2/file_is_dupe2']
        }
        assert deltable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/file_exists',
            '/tests/tdata/somedir/file_is_dupe']
        }
    
    
    def test_dedupe_within_dupes_global_off_autoselect_off(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = ds.dd3(
            deldir, "file_", dupes_global = False, match_deletions = True, autoselect = False)
        assert deltable == {}
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/file_is_dupe',
            '/tests/tdata/somedir/somedir2/file_is_dupe2']
        }
    
    
    def test_dedupe_within_dupes_global_off_autoselect_on(self):
        ds = Dupetable(dbname = os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        deltable, keeptable = ds.dd3(
            deldir, "file_", dupes_global = False, match_deletions = True, autoselect = True)
        assert deltable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/file_is_dupe']
        }
        assert keeptable == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/somedir/somedir2/file_is_dupe2']
        }
