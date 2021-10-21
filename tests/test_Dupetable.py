import os
import pathlib
import tempfile
from typing import DefaultDict

import pydupe.dupetable as dupetable
import pytest
import copy
from pydupe.db import PydupeDB

cwd = str(pathlib.Path.cwd())
tdata = cwd + "/pydupe/pydupe/tests/tdata/"
home = str(pathlib.Path.home())

def lu_to_str(lutable):
    str_table = DefaultDict(list)
    for k in lutable.keys():
        for v in lutable[k]:
            str_table[k].append(str(v))
    return str_table

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
class TestDupetable:

    def test_Dupetable_basic(self):
        hashlu = dupetable.get_dupes(dbname=os.getcwd() + '/.dbtest.sqlite')
        assert lu_to_str(hashlu) == {
            'be1c1a22b4055523a0d736f4174ef1d6': [
                '/tests/tdata/file_exists',
                '/tests/tdata/somedir/file_is_dupe'],
            '3aa2ed13ee40ba651e87a0fd60b753d0': [
                '/tests/tdata/somedir/dupe_in_dir',
                '/tests/tdata/somedir/dupe2_in_dir']
        }

    def test_Dupetable_tables(self):
        hashlu = dupetable.get_dupes(dbname=os.getcwd() + '/.dbtest.sqlite')
        deldir = "/tests/tdata/somedir"
        d, k = dupetable.dd3(hashlu, deldir, pattern="_dupe", dupes_global=True)
        assert lu_to_str(d) == {
            'be1c1a22b4055523a0d736f4174ef1d6':
                ['/tests/tdata/somedir/file_is_dupe']}
        assert lu_to_str(k) == {
            'be1c1a22b4055523a0d736f4174ef1d6':
            ['/tests/tdata/file_exists']
        }

    def test_dir_counter(self):
        hashlu = dupetable.get_dupes(dbname=os.getcwd() + '/.dbtest.sqlite')
        dir_counter = dupetable.get_dir_counter(hashlu)
        assert dir_counter == {
            '/tests/tdata': 1,
            '/tests/tdata/somedir': 3
        }
