import pathlib
import tempfile
import platform

import pytest
from pydupe.db import PydupeDB
from pydupe.hasher import Hasher

@pytest.fixture(scope = 'function')
def get_hashexecute():
    if (systm:= platform.system()) == 'Linux':
        HASHEXECUTE = ['sha256sum']
    elif systm == 'FreeBSD':
        HASHEXECUTE = ['shasum', '-a 256']
    yield HASHEXECUTE

@pytest.fixture(scope='function')
def setup_tmp_path():
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        somedir = pathlib.Path(tmpdirname) / 'somedir'
        somedir.mkdir()
        somefile = somedir / 'somefile.txt'
        with somefile.open('w') as f:
            f.write('sometext')

        somedir2 = somedir / 'somedir2'
        somedir2.mkdir()

        somefile1 = somedir2 / 'file1'
        somefile2 = somedir2 / 'file2'
        somefile3 = somedir2 / 'file3'
        somefile4 = somedir2 / 'file4'
        somefile5 = somedir2 / 'file5'
        somefile6 = somedir2 / 'file6'

        somefile1.write_text('some content 1')
        somefile2.write_text('some content 2')
        somefile3.write_text('some content 3')
        somefile4.write_text('some content 4')
        somefile5.write_text('some content 5')
        somefile6.write_text('some content 6')

        yield tmpdirname


def sqlite3_dictsort(lst):
    return sorted(lst, key=lambda k: k['filename'])

# @pytest.mark.usefixtures("setup_tmp_path")


class TestHasher:
    def test_fixture(self, setup_tmp_path):
        tmpdirname = setup_tmp_path
        dbname = tmpdirname + "/.test_Hasher.sqlite"
        path = pathlib.Path(tmpdirname + "/somedir")
        hsr = Hasher(dbname)
        hsr.scan_files_on_disk_and_insert_stats_in_db(path)
        data_should = []
        for item in path.rglob("*"):
            if item.is_file():
                dic = {}
                size, inode, mtime, ctime = hsr.get_stats_of_file(item)
                dic['filename'] = str(item)
                dic['hash'] = None
                dic['size'] = size
                dic['inode'] = inode
                dic['mtime'] = mtime
                dic['ctime'] = ctime
                data_should.append(dic)

        data_get = [dict(d) for d in PydupeDB(dbname).get().fetchall()]

        assert sqlite3_dictsort(data_get) == sqlite3_dictsort(data_should)

    def test_move_dbcontent_for_dir_to_tmp(self, setup_tmp_path):
        tmpdirname = setup_tmp_path
        dbname = tmpdirname + "/.test_Hasher.sqlite"
        path = pathlib.Path(tmpdirname + "/somedir")
        path_2 = pathlib.Path(tmpdirname + "/somedir/somedir2")
        path_1 = pathlib.Path(tmpdirname + "/somedir/somefile.txt")
        hsr = Hasher(dbname)
        hsr.scan_files_on_disk_and_insert_stats_in_db(path)
        hsr.move_dbcontent_for_dir_to_tmp(str(path_2))

        data_should_tmp = []
        for item in path_2.rglob("*"):
            if item.is_file():
                dic = {}
                size, inode, mtime, ctime = hsr.get_stats_of_file(item)
                dic['filename'] = str(item)
                dic['hash'] = None
                dic['size'] = size
                dic['inode'] = inode
                dic['mtime'] = mtime
                dic['ctime'] = ctime
                data_should_tmp.append(dic)

        data_should_lookup = []
        item = path_1
        dic = {}
        size, inode, mtime, ctime = hsr.get_stats_of_file(item)
        dic['filename'] = str(item)
        dic['hash'] = None
        dic['size'] = size
        dic['inode'] = inode
        dic['mtime'] = mtime
        dic['ctime'] = ctime
        data_should_lookup.append(dic)

        sql_execute_tmp = "SELECT * FROM tmp"
        sql_execute_lookup = "SELECT * FROM lookup"

        data_get_tmp = [dict(d) for d in PydupeDB(
            dbname).execute(sql_execute_tmp).fetchall()]
        data_get_lookup = [dict(d) for d in PydupeDB(
            dbname).execute(sql_execute_lookup).fetchall()]

        assert sqlite3_dictsort(
            data_get_lookup) == sqlite3_dictsort(data_should_lookup)
        assert sqlite3_dictsort(
            data_get_tmp) == sqlite3_dictsort(data_should_tmp)

    def test_copy_hash_from_tmp_if_unchanged_inode_size_mtim_ctime(self):
        """ this function is composed of atomic PydupeDB methods that have been tested"""
        pass

    def test_rehash_rows_where_hash_is_NULL(self, setup_tmp_path, get_hashexecute):
        tmpdirname = setup_tmp_path
        dbname = tmpdirname + "/.test_Hasher.sqlite"
        path = pathlib.Path(tmpdirname + "/somedir/somedir2")
        hsr = Hasher(dbname)
        hsr.scan_files_on_disk_and_insert_stats_in_db(path)

        somefile1 = path / 'file1'
        somefile2 = path / 'file2'
        somefile3 = path / 'file3'
        somefile4 = path / 'file4'
        somefile5 = path / 'file5'
        somefile6 = path / 'file6'
        filelist = [somefile1, somefile2, somefile3,
                    somefile4, somefile5, somefile6]
        hashlist = [hsr.hash_file(f, HASHEXECUTE = get_hashexecute) for f in filelist]

        with PydupeDB(dbname) as db:
            for file, hash in zip(filelist, hashlist):
                db.update_hash(str(file), hash)
            db.commit()

        with PydupeDB(dbname) as db:
            data_before = [dict(d) for d in db.get().fetchall()]

        with PydupeDB(dbname) as db:
            for file in filelist:
                db.update_hash(str(file), None)
            db.commit()

        hsr.rehash_rows_where_hash_is_NULL()

        with PydupeDB(dbname) as db:
            data_after = [dict(d) for d in db.get().fetchall()]

        assert sqlite3_dictsort(
            data_before) == sqlite3_dictsort(data_after)
