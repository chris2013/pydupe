import os
import pathlib as pl
import tempfile
import typing as tp

import pydupe.dupetable as dupetable
import pytest
from pydupe.db import PydupeDB
from pydupe.data import fparms

cwd = str(pl.Path.cwd())
tdata = cwd + "/pydupe/pydupe/tests/tdata/"
home = str(pl.Path.home())

@pytest.fixture
def setup_database() -> tp.Generator[tp.Any, tp.Any, tp.Any]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        dbname = pl.Path.cwd() / ".dbtest.sqlite"
        data = [
            fparms(filename='/tests/tdata/file_exists',
             hash='be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             size=1,
             inode=25303464,
             mtime=1629356592,
             ctime=1630424506),
            fparms(filename='/tests/tdata/somedir/file_is_dupe',
             hash='be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             size=1,
             inode=25303464,
             mtime=1629356592,
             ctime=1630424506),
            fparms(filename='/tests/tdata/somedir/dupe_in_dir',
             hash='3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             size=1,
             inode=25303464,
             mtime=1629356592,
             ctime=1630424506),
            fparms(filename='/tests/tdata/somedir/dupe2_in_dir',
             hash='3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             size=1,
             inode=25303464,
             mtime=1629356592,
             ctime=1630424506)]

        with PydupeDB(dbname) as db:
            db.parms_insert(data)
            db.commit()

        yield

        os.chdir(old_cwd)


@pytest.mark.usefixtures("setup_database")
class TestDupetable:

    def test_Dupetable_basic(self) -> None:
        hashlu = dupetable.get_dupes(dbname=pl.Path.cwd() / '.dbtest.sqlite')
        assert hashlu.as_dict_of_strsets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {
                '/tests/tdata/file_exists',
                '/tests/tdata/somedir/file_is_dupe'},
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {
                '/tests/tdata/somedir/dupe2_in_dir',
                '/tests/tdata/somedir/dupe_in_dir'}
        }

    def test_Dupetable_tables(self) -> None:
        hashlu = dupetable.get_dupes(dbname=pl.Path.cwd() / '.dbtest.sqlite')
        deldir = pl.Path("/tests/tdata/somedir")
        d, k = dupetable.dd3(hashlu, deldir=deldir, pattern="_dupe", match_deletions=True, dupes_global=True, autoselect=False)
        assert d.as_dict_of_strsets() == {
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6':
                {'/tests/tdata/somedir/file_is_dupe'}}
        assert k.as_dict_of_strsets() =={
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {'/tests/tdata/somedir/dupe2_in_dir', '/tests/tdata/somedir/dupe_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {'/tests/tdata/file_exists'}
        }

    def test_dir_counter(self) -> None:
        Dp = dupetable.Dupes(dbname=pl.Path.cwd() / '.dbtest.sqlite')
        dir_counter = Dp.get_dir_counter()
        assert dir_counter == {
            '/tests/tdata': 1,
            '/tests/tdata/somedir': 3
        }

    def test_raise_if_all_files_marked_for_deletion(self) -> None:
        Dp = dupetable.Dupetable(dbname=pl.Path.cwd() / '.dbtest.sqlite', deldir=pl.Path("/"), pattern=".", autoselect=True, dedupe=True)
        
        assert Dp.dupes.as_dict_of_strsets() == {
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {
                '/tests/tdata/somedir/dupe2_in_dir',
                '/tests/tdata/somedir/dupe_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {
                '/tests/tdata/file_exists',
                '/tests/tdata/somedir/file_is_dupe'}}
       
        assert Dp._deltable.as_dict_of_strsets() == {
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {
                '/tests/tdata/somedir/dupe2_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {
                '/tests/tdata/file_exists'}}
       
        assert Dp._keeptable.as_dict_of_strsets() == {
            '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0': {
                '/tests/tdata/somedir/dupe_in_dir'},
            'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6': {
                '/tests/tdata/somedir/file_is_dupe'}}
        
        Dp._deltable |= Dp._keeptable

        with pytest.raises(dupetable.DupeNotValidated) as execinfo:   
            Dp.validate()    
