import pathlib as pl
import tempfile
import typing as tp

import pytest
from pydupe.config import cnf
from pydupe.db import PydupeDB
from pydupe.data import fparms, from_path, from_row
import pydupe.hasher 

@pytest.fixture(scope='function')
def setup_tmp_path() -> tp.Generator[tp.Any, tp.Any, tp.Any]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        somedir = pl.Path(tmpdirname) / 'somedir'
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


def sqlite3_dictsort(lst: tp.List[dict[str, str]])-> tp.List[dict[str, str]]:
    return sorted(lst, key=lambda k: k['filename'])

# @pytest.mark.usefixtures("setup_tmp_path")

class TestHasher:
    def test_fixture_and_from_row(self, setup_tmp_path: str) -> None:
        tmpdirname = setup_tmp_path
        dbname = pl.Path(tmpdirname + "/.test_Hasher.sqlite")
        path = pl.Path(tmpdirname + "/somedir")
        pydupe.hasher.clean(dbname)
        pydupe.hasher.scan_files_on_disk_and_insert_stats_in_db(dbname, path)
        data_should: tp.List[tp.Optional[fparms]] = []
        for item in path.rglob("*"):
            data_should.append(from_path(item))

        data_get = [from_row(d) for d in PydupeDB(dbname).get().fetchall()]

        assert data_get.sort() == data_should.sort()

    def test_move_dbcontent_for_dir_to_permanent(self, setup_tmp_path: str) -> None:
        tmpdirname = setup_tmp_path
        dbname = pl.Path(tmpdirname + "/.test_Hasher.sqlite")
        path = pl.Path(tmpdirname + "/somedir")
        path_2 = pl.Path(tmpdirname + "/somedir/somedir2")
        path_1 = pl.Path(tmpdirname + "/somedir/somefile.txt")
        pydupe.hasher.clean(dbname)
        pydupe.hasher.scan_files_on_disk_and_insert_stats_in_db(dbname, path)
        PydupeDB(dbname).move_dbcontent_for_dir_to_permanent(path_2)

        data_should_permanent: tp.List[tp.Optional[fparms]] = []
        for item in path_2.rglob("*"):
            data_should_permanent.append(from_path(item))

        data_should_lookup: tp.List[tp.Optional[fparms]] = []
        data_should_lookup.append(from_path(path_1)) 

        sql_execute_permanent = "SELECT * FROM permanent"
        sql_execute_lookup = "SELECT * FROM lookup"

        data_get_permanent = [from_row(d) for d in PydupeDB(
            dbname).execute(sql_execute_permanent).fetchall()]
        data_get_lookup = [from_row(d) for d in PydupeDB(
            dbname).execute(sql_execute_lookup).fetchall()]

        assert data_get_lookup.sort() == data_should_lookup.sort()
        assert data_get_permanent.sort() == data_should_permanent.sort()

    def test_copy_dbcontent_for_dir_to_permanent(self, setup_tmp_path: str) -> None:
        tmpdirname = setup_tmp_path
        dbname = pl.Path(tmpdirname + "/.test_Hasher.sqlite")
        path = pl.Path(tmpdirname + "/somedir")
        path_2 = pl.Path(tmpdirname + "/somedir/somedir2")
        path_1 = pl.Path(tmpdirname + "/somedir/somefile.txt")
        pydupe.hasher.clean(dbname)
        pydupe.hasher.scan_files_on_disk_and_insert_stats_in_db(dbname, path)
        PydupeDB(dbname).copy_dbcontent_for_dir_to_permanent(path_2)

        data_should_permanent: tp.List[tp.Optional[fparms]] = []
        for item in path_2.rglob("*"):
            data_should_permanent.append(from_path(item))

        data_should_lookup: tp.List[tp.Optional[fparms]] = []
        data_should_lookup.append(from_path(path_1)) 

        sql_execute_permanent = "SELECT * FROM permanent"
        sql_execute_lookup = "SELECT * FROM lookup"

        data_get_permanent = [from_row(d) for d in PydupeDB(
            dbname).execute(sql_execute_permanent).fetchall()]
        data_get_lookup = [from_row(d) for d in PydupeDB(
            dbname).execute(sql_execute_lookup).fetchall()]

        assert data_get_lookup.sort() == data_should_lookup.sort()
        assert data_get_permanent.sort() == data_should_permanent.sort()


    def notest_rehash_rows_where_hash_is_NULL(self, setup_tmp_path: str) -> None:
        tmpdirname = setup_tmp_path
        dbname = pl.Path(tmpdirname + "/.test_Hasher.sqlite")
        path = pl.Path(tmpdirname + "/somedir/somedir2")
        pydupe.hasher.clean(dbname)
        pydupe.hasher.scan_files_on_disk_and_insert_stats_in_db(dbname, path)

        somefile1 = path / 'file1'
        somefile2 = path / 'file2'
        somefile3 = path / 'file3'
        somefile4 = path / 'file4'
        somefile5 = path / 'file5'
        somefile6 = path / 'file6'
        filelist = [somefile1, somefile2, somefile3,
                    somefile4, somefile5, somefile6]
        hashlist = [pydupe.hasher.hash_file(str(f)) for f in filelist]

        with PydupeDB(dbname) as db:
            for file, hash in zip(filelist, hashlist):
                db.update_hash([(hash, str(file))])
            db.commit()

        with PydupeDB(dbname) as db:
            data_before = [dict(d) for d in db.get().fetchall()]

        with PydupeDB(dbname) as db:
            for file in filelist:
                db.update_hash([(None, str(file))])
            db.commit()

        pydupe.hasher.rehash_dupes_where_hash_is_NULL(dbname)

        with PydupeDB(dbname) as db:
            data_after = [dict(d) for d in db.get().fetchall()]

        assert sqlite3_dictsort(
            data_before) == sqlite3_dictsort(data_after)
