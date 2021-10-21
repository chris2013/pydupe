import os
import tempfile
import pytest

from pydupe.db import PydupeDB


@pytest.fixture
def setup_database():
    """ Fixture to se-t up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        dbname = os.getcwd() + "/.dbtest.sqlite"
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
class TestDatabase:

    def test_insert_get(self):
        """check data inserted in fixture 'setup_database' works."""
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"
        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
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

    def test_update_hash(self):
        """check hash update works."""
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"

        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        # before:
        assert data_dict == [
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
        # update hash:
        with PydupeDB(dbname) as db:
            filename = '/tests/tdata/somedir/dupe2_in_dir'
            hash = '3aa2ed13ee40ba651e87a0fd60bxxxxx'
            db.update_hash(filename, hash)
            db.commit()
        # after:
        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
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
             'hash': '3aa2ed13ee40ba651e87a0fd60bxxxxx',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

    def test_rollback(self):
        """check autorolback after e.g. hash update works."""
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"

        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        # before:
        assert data_dict == [
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
        # update hash:
        with PydupeDB(dbname) as db:
            filename = '/tests/tdata/somedir/dupe2_in_dir'
            hash = '3aa2ed13ee40ba651e87a0fd60bxxxxx'
            db.update_hash(filename, hash)
            # no commit -> auto rollback by context manager

        with PydupeDB(dbname) as db:
            data_get = db.get().fetchall()
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
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

    def test_get_list_of_files_where_hash_is_NULL(self):
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"
        data = {
            'filename': '/tests/tdata/file_exists',
            'hash': None,
            'size': 1,
            'inode': 25303464,
            'mtime': 1629356592,
            'ctime': 1630424506}
        ftuple = (data['filename'], data['hash'], data['size'],
                  data['inode'], data['mtime'], data['ctime'])

        with PydupeDB(dbname) as db:
            db.execute(
                "DELETE from lookup WHERE hash = 'be1c1a22b4055523a0d736f4174ef1d6'")
            db.insert(ftuple)
            db.commit()
            data_get = db.get_list_of_equal_sized_files_where_hash_is_NULL()

        assert data_get == [
            '/tests/tdata/file_exists']

    def test_get_list_of_files_in_dir(self):
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"

        with PydupeDB(dbname) as db:
            data_get = db.get_list_of_files_in_dir(
                '/tests/tdata/somedir')

        assert data_get == ['/tests/tdata/somedir/dupe2_in_dir',
                            '/tests/tdata/somedir/dupe_in_dir',
                            '/tests/tdata/somedir/file_is_dupe'
                            ]

    def test_get_file_hash(self):
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"

        with PydupeDB(dbname) as db:
            data_get = db.get_file_hash()
            data_dict = [dict(row) for row in data_get]

        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6'},
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6'},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d0'},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d0'}
        ]

    def test_delete_dir(self):
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"

        with PydupeDB(dbname) as db:
            db.delete_dir('/tests/tdata/somedir')
            data_get = db.get().fetchall()
        
        data_dict = [dict(row) for row in data_get]
        assert data_dict == [
            {'filename': '/tests/tdata/file_exists',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6',
             'size': 1,
             'inode': 25303464,
             'mtime': 1629356592,
             'ctime': 1630424506}]

    def test_delete_file(self):
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"

        with PydupeDB(dbname) as db:
            db.delete_file(filename='/tests/tdata/file_exists')
            data_get = db.get_file_hash().fetchall()
        
        data_dict = [dict(row) for row in data_get]

        assert data_dict == [
            {'filename': '/tests/tdata/somedir/file_is_dupe',
             'hash': 'be1c1a22b4055523a0d736f4174ef1d6'},
            {'filename': '/tests/tdata/somedir/dupe_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d0'},
            {'filename': '/tests/tdata/somedir/dupe2_in_dir',
             'hash': '3aa2ed13ee40ba651e87a0fd60b753d0'}
        ]

    def test_copy_dir_to_table_tmp(self):
        """check data inserted in fixture 'setup_database' works."""
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"
        with PydupeDB(dbname) as db:
            db.copy_dir_to_table_tmp(
                '/tests/tdata/somedir')
            db.commit()
            data_get_lookup = db.execute('SELECT * FROM lookup').fetchall()
            data_get_tmp = db.execute('SELECT * FROM tmp').fetchall()
        data_dict_lookup = [dict(row) for row in data_get_lookup]
        data_dict_tmp = [dict(row) for row in data_get_tmp]
        assert data_dict_lookup == [
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
        assert data_dict_tmp == [
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
    
    def test_copy_hash_to_table_lookup_and_clear_tmp(self):
        """check data inserted in fixture 'setup_database' works."""
        cwd = os.getcwd()
        dbname = cwd + "/.dbtest.sqlite"
        with PydupeDB(dbname) as db:
            db.execute("INSERT INTO tmp SELECT * FROM lookup WHERE filename like '/tests/tdata/somedir%'")
            db.update_hash(
                '/tests/tdata/file_exists', hash = None)
            db.update_hash(
                '/tests/tdata/somedir/file_is_dupe', hash = None)
            db.commit()
            data_get_lookup = db.execute('SELECT * FROM lookup').fetchall()
            data_get_tmp = db.execute('SELECT * FROM tmp').fetchall()
        data_dict_lookup = [dict(row) for row in data_get_lookup]
        data_dict_tmp = [dict(row) for row in data_get_tmp]
        
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
        assert data_dict_tmp == [
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
            db.copy_hash_to_table_lookup(check_filename=True)
            db.commit()
            data_get_lookup = db.execute('SELECT * FROM lookup').fetchall()
            data_get_tmp = db.execute('SELECT * FROM tmp').fetchall()

        data_dict_lookup = [dict(row) for row in data_get_lookup]
        data_dict_tmp = [dict(row) for row in data_get_tmp]

        assert data_dict_lookup == [
            {'filename': '/tests/tdata/file_exists',
             'hash': None,
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
        assert data_dict_tmp == [
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
            db.clear_tmp()
            db.commit()
            data_get_tmp = db.execute('SELECT * FROM tmp').fetchall()

        data_dict_tmp = [dict(row) for row in data_get_tmp]
        
        assert data_dict_tmp == []