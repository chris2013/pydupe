import os
import tempfile
import pytest
from pydupe.data import fparms
from pydupe.db import PydupeDB
from pathlib import Path as p
import typing as tp

@pytest.fixture
def setup_database() -> tp.Generator[None,None,None]:
    """ Fixture to se-t up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        dbname = p.cwd() / ".dbtest.sqlite"
        data = [fparms(filename='/tests/tdata/file_exists',
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
class TestDatabase:

    def test_insert_get(self) -> None:
        """check data inserted in fixture 'setup_database' works."""
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

    def test_update_hash(self) -> None:
        """check hash update works."""
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        # before:
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
        # update hash:
        with PydupeDB(dbname) as db:
            filename = '/tests/tdata/somedir/dupe2_in_dir'
            hash = '3aa2ed13ee40ba651e87a0fd60bbbbbb3aa2ed13ee40ba651e87a0fd60bbbbbb'
            db.update_hash([(hash, filename)])
            db.commit()
        # after:
        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60bbbbbb3aa2ed13ee40ba651e87a0fd60bbbbbb',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

    def test_rollback(self) -> None:
        """check autorolback after e.g. hash update works."""
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        # before:
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
        # update hash:
        with PydupeDB(dbname) as db:
            filename = '/tests/tdata/somedir/dupe2_in_dir'
            hash = '3aa2ed13ee40ba651e87a0fd60bbbbbb3aa2ed13ee40ba651e87a0fd60bbbbbb'
            db.update_hash([(hash, filename)])
            # no commit -> auto rollback by context manager

        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

    def test_get_list_of_files_where_hash_is_NULL(self) -> None:
        
        dbname = p.cwd() / ".dbtest.sqlite"
        data = [fparms(
            filename='/tests/tdata/file_exists',
            hash=None,
            size=1,
            inode=25303464,
            mtime=1629356592,
            ctime=1630424506)]

        with PydupeDB(dbname) as db:
            db.execute(
                "DELETE from lookup WHERE hash = 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6'")
            db.parms_insert(data)
            db.commit()
            data_get = db.get_list_of_equal_sized_files_where_hash_is_NULL()

        assert data_get == [
            '/tests/tdata/file_exists']

    def test_get_list_of_files_in_dir(self) -> None:
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            data_get = db.get_list_of_files_in_dir(
                '/tests/tdata/somedir')

        assert data_get == ['/tests/tdata/somedir/dupe2_in_dir',
                            '/tests/tdata/somedir/dupe_in_dir',
                            '/tests/tdata/somedir/file_is_dupe'
                            ]

    def test_get_file_hash(self) -> None:
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            data_get = db.get_file_hash()
            data_dict = [dict(row) for row in data_get]

        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6'},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6'},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0'},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0'}
        ]

    def test_delete_dir(self) -> None:
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            db.delete_dir(p('/tests/tdata/somedir'))
            data_get = db.get().fetchall()
        
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

    def test_delete_file(self) -> None:
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            db.delete_file_lookup(filename=p('/tests/tdata/file_exists'))
            data_get = db.get_file_hash().fetchall()
        
        data_dict = [dict(row) for row in data_get]

        assert data_dict == [
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6'},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0'},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0'}
        ]

    def test_copy_dir_to_table_permanent(self) -> None:
        """check data inserted in fixture 'setup_database' works."""
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            db.copy_dir_to_table_permanent(
                p('/tests/tdata/somedir'))
            db.commit()
            data_get_lookup = db.execute('SELECT * FROM lookup').fetchall()
            data_get_permanent = db.execute('SELECT * FROM permanent').fetchall()
        data_dict_lookup = [dict(row) for row in data_get_lookup]
        data_dict_permanent = [dict(row) for row in data_get_permanent]
        assert data_dict_lookup == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
        assert data_dict_permanent == [
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
    
    def test_copy_hash_to_table_lookup_and_clear_permanent(self) -> None:
        """check data inserted in fixture 'setup_database' works."""
        
        dbname = p.cwd() / ".dbtest.sqlite"
        with PydupeDB(dbname) as db:
            db.execute("INSERT INTO permanent SELECT * FROM lookup WHERE filename like '/tests/tdata/somedir%'")
            db.update_hash([(None, '/tests/tdata/file_exists')])
            db.update_hash([(None, '/tests/tdata/somedir/file_is_dupe')])
            db.commit()
            data_get_lookup = db.execute('SELECT * FROM lookup').fetchall()
            data_get_permanent = db.execute('SELECT * FROM permanent').fetchall()
        data_dict_lookup = [dict(row) for row in data_get_lookup]
        data_dict_permanent = [dict(row) for row in data_get_permanent]
        
        assert data_dict_lookup == [
            {'filename': '/tests/tdata/file_exists',
             'hash': None,
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': None,
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
        assert data_dict_permanent == [
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

        with PydupeDB(dbname) as db:
            db.copy_hash_to_table_lookup()
            db.commit()
            data_get_lookup = db.execute('SELECT * FROM lookup').fetchall()
            data_get_permanent = db.execute('SELECT * FROM permanent').fetchall()

        data_dict_lookup = [dict(row) for row in data_get_lookup]
        data_dict_permanent = [dict(row) for row in data_get_permanent]

        assert data_dict_lookup == [
            {'filename': '/tests/tdata/file_exists',
             'hash': None,
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
        assert data_dict_permanent == [
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d03aa2ed13ee40ba651e87a0fd60b753d0',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]
