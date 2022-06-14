import pathlib
import sqlite3
import typing as tp
import types as ty
from pydupe.data import fparms

class PydupeDB(object):
    """
    sqlite3 database class for pydupe. 
    use as a context manager:
        with Database() as db:
            db.create_table()
            db.commit

    PydupeDB context manager opens an implicit transaction and does a default rollback.
    Every change needs to be explicitely comitted!
    """

    def __init__(self, dbname: pathlib.Path = pathlib.Path.home() / ".pydupe.sqlite"):
        self._dbname = str(dbname)
        self.connection = sqlite3.connect(self._dbname)
        self.connection.row_factory = sqlite3.Row
        self.cur = self.connection.cursor()
        create_table_if_not_exist_sql = """
                            CREATE TABLE IF NOT EXISTS lookup (
                            filename TEXT PRIMARY KEY,
                            hash TEXT,
                            size INTEGER,
                            inode INTEGER,
                            mtime INTEGER,
                            ctime INTEGER)"""
        self.execute(create_table_if_not_exist_sql)
        create_table_if_not_exist_sql = """
        CREATE TABLE IF NOT EXISTS permanent (
                            filename TEXT PRIMARY KEY,
                            hash TEXT,
                            size INTEGER,
                            inode INTEGER,
                            mtime INTEGER,
                            ctime INTEGER)"""
        self.execute(create_table_if_not_exist_sql)
        self.commit()

    def __enter__(self) -> 'PydupeDB' :
        return self

    def __exit__(self, ext_type: tp.Optional[tp.Type[BaseException]], exc_value: tp.Optional[BaseException], traceback: tp.Optional[ty.TracebackType]) -> tp.Optional[bool]:
        self.cur.close()
        self.connection.rollback()  # rollback by default!
        self.connection.close()
        return ext_type is None

    def parms_insert(self,item: list[fparms]) -> None:
        list_of_tupls=[(fparm.filename, fparm.hash, fparm.size, fparm.inode, fparm.mtime, fparm.ctime) for fparm in item]
        insert_sql = "INSERT INTO lookup (filename, hash, size, inode, mtime, ctime) VALUES (?,?,?,?,?,?)"
        self.cur.executemany(insert_sql, list_of_tupls) 

    def update_hash(self, list_of_tupl: list[tuple[tp.Optional[str],str]])-> None:
        update_sql = "UPDATE lookup SET hash = ? where filename = ?"
        self.cur.executemany(update_sql, list_of_tupl)

    def get_list_of_equal_sized_files_where_hash_is_NULL(self) -> tp.List[str]:
        # select files with same size with no hash yet
        get_sql = "SELECT l.filename FROM lookup l JOIN (SELECT size, count(*) c FROM lookup GROUP BY size HAVING c > 1) s on l.size = s.size where l.hash is NULL"
        data_get = self.cur.execute(get_sql)
        return [d['filename'] for d in data_get]

    def get_dupes(self) -> sqlite3.Cursor:
        get_sql = "SELECT l.filename, l.hash FROM lookup l JOIN (SELECT hash, count(*) c FROM lookup GROUP BY hash HAVING c > 1) h on l.hash = h.hash order by l.hash"
        self.cur.execute(get_sql)
        return self.cur

    def delete_dir(self, dirname: pathlib.Path) -> None:
        dirname_str: str = str(dirname)
        delete_sql = "DELETE FROM lookup WHERE filename LIKE ?"
        self.cur.execute(delete_sql, (dirname_str + '%',))

    def delete_file(self, filename: pathlib.Path) -> None:
        delete_sql = "DELETE from lookup where filename like ?"
        self.cur.execute(delete_sql, (str(filename),))

    def copy_dir_to_table_permanent(self, dirname: pathlib.Path) -> None:
        dirname_str: str = str(dirname)
        copy_sql = "REPLACE INTO permanent select * FROM lookup WHERE filename LIKE ?"
        self.cur.execute(copy_sql, (dirname_str + '%',))

    def copy_hash_to_table_lookup(self, *, check_filename: bool=True) -> None:
        if check_filename:
            updateLookup_sql = """
            UPDATE lookup
            SET hash = permanent.hash
            FROM permanent
            WHERE
                permanent.size = lookup.size AND
                permanent.inode = lookup.inode AND
                permanent.ctime = lookup.ctime AND
                permanent.mtime = lookup.mtime AND
                permanent.filename = lookup.filename
            """
        else:
            updateLookup_sql = """
            UPDATE lookup
            SET hash = permanent.hash
            FROM permanent
            WHERE
                permanent.size = lookup.size AND
                permanent.inode = lookup.inode AND
                permanent.ctime = lookup.ctime AND
                permanent.mtime = lookup.mtime
            """
        self.cur.execute(updateLookup_sql)

    def execute(self, sql: str) -> sqlite3.Cursor:
        return self.cur.execute(sql)

    def commit(self) -> None:
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def purge(self) -> None:
        copy_sql = "REPLACE INTO permanent select * FROM lookup"
        self.cur.execute(copy_sql)
        delete_sql = "DELETE from lookup"
        self.cur.execute(delete_sql)
        self.commit()

    # for testing only
    def get(self) -> sqlite3.Cursor:
        get_sql = "SELECT * FROM lookup"
        return self.cur.execute(get_sql)

    # for testing only
    def get_list_of_files_in_dir(self, dirname: str) -> tp.List[str]:
        get_sql = "SELECT filename FROM lookup WHERE filename LIKE ?"
        data_get = self.cur.execute(get_sql, (dirname + '%',))
        return [d['filename'] for d in data_get]

    # for testing only
    def get_file_hash(self) -> sqlite3.Cursor:
        get_sql = "SELECT filename, hash from lookup"
        self.cur.execute(get_sql)
        return self.cur

    def move_dbcontent_for_dir_to_permanent(self, path: pathlib.Path) -> None:
        self.copy_dir_to_table_permanent(path)
        self.delete_dir(path)
        self.commit()

    def copy_hash_from_permanent_if_unchanged_inode_size_mtime_ctime(self) -> None:
        self.copy_hash_to_table_lookup()
        self.commit()

    def get_dupes_where_hash_is_NULL(self) -> tp.List[str]:
        list_of_files_to_update: tp.List[str] = self.get_list_of_equal_sized_files_where_hash_is_NULL()
        return list_of_files_to_update


