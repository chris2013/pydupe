import pathlib
import re
import sqlite3

from pydupe.console import console


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

    def __init__(self, dbname=str(pathlib.Path.home()) + "/" + ".pydupe.sqlite"):
        self._dbname = dbname
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
        CREATE TABLE IF NOT EXISTS tmp (
                            filename TEXT PRIMARY KEY,
                            hash TEXT,
                            size INTEGER,
                            inode INTEGER,
                            mtime INTEGER,
                            ctime INTEGER)"""
        self.execute(create_table_if_not_exist_sql)
        self.commit()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cur.close()
        self.connection.rollback()  # rollback by default!
        self.connection.close()

    def insert(self, ftuple):
        insert_sql = "INSERT INTO lookup (filename, hash, size, inode, mtime, ctime) VALUES (?,?,?,?,?,?)"
        self.cur.execute(insert_sql, ftuple)

    def get(self):
        get_sql = "SELECT * FROM lookup"
        return self.cur.execute(get_sql)

    def update_hash(self, filename, hash):
        update_sql = "UPDATE lookup SET hash = ? where filename = ?"
        self.cur.execute(update_sql, (hash, filename))

    def get_list_of_equal_sized_files_where_hash_is_NULL(self):
        # select files with same size with no hash yet
        get_sql = "SELECT l.filename FROM lookup l JOIN (SELECT size, count(*) c FROM lookup GROUP BY size HAVING c > 1) s on l.size = s.size where l.hash is NULL"
        data_get = self.cur.execute(get_sql)
        return [d['filename'] for d in data_get]

    def get_list_of_files_in_dir(self, dirname):
        get_sql = "SELECT filename FROM lookup WHERE filename LIKE ?"
        data_get = self.cur.execute(get_sql, (dirname + '%',))
        return [d['filename'] for d in data_get]

    def get_file_hash(self):
        get_sql = "SELECT filename, hash from lookup"
        self.cur.execute(get_sql)
        return self.cur

    def get_dupes(self):
        get_sql = "SELECT l.filename, l.hash FROM lookup l JOIN (SELECT hash, count(*) c FROM lookup GROUP BY hash HAVING c > 1) h on l.hash = h.hash order by l.hash"
        self.cur.execute(get_sql)
        return self.cur

    def delete_dir(self, dirname):
        delete_sql = "DELETE FROM lookup WHERE filename LIKE ?"
        self.cur.execute(delete_sql, (dirname + '%',))

    def reduce_to_dir(self, dirname):
        copy_sql = "INSERT INTO tmp select * FROM lookup WHERE filename NOT LIKE ?"
        self.cur.execute(copy_sql, (dirname + '%',))

        delete_sql = "DELETE FROM lookup WHERE filename NOT like ?"
        self.cur.execute(delete_sql, (dirname + '%',))

    def delete_file(self, filename: str):
        delete_sql = "DELETE from lookup where filename like ?"
        self.cur.execute(delete_sql, (filename,))

    def copy_dir_to_table_tmp(self, dirname):
        copy_sql = "INSERT INTO tmp select * FROM lookup WHERE filename LIKE ?"
        self.cur.execute(copy_sql, (dirname + '%',))

    def copy_hash_to_table_lookup(self, *, check_filename=True):
        if check_filename:
            updateLookup_sql = """
            UPDATE lookup
            SET hash = tmp.hash
            FROM tmp
            WHERE
                tmp.size = lookup.size AND
                tmp.inode = lookup.inode AND
                tmp.ctime = lookup.ctime AND
                tmp.mtime = lookup.mtime AND
                tmp.filename = lookup.filename
            """
        else:
            updateLookup_sql = """
            UPDATE lookup
            SET hash = tmp.hash
            FROM tmp
            WHERE
                tmp.size = lookup.size AND
                tmp.inode = lookup.inode AND
                tmp.ctime = lookup.ctime AND
                tmp.mtime = lookup.mtime
            """
        self.cur.execute(updateLookup_sql)

    def sanitize_hash_regex(self):
        regex = "[A-Fa-f0-9]{64}"

        def regexp(expr, item):
            reg = re.compile(expr)
            return reg.search(item) is not None
        self.connection.create_function("REGEXP", 2, regexp)

        select_sql = 'DELETE FROM lookup WHERE NOT hash REGEXP ?'
        self.cur.execute(select_sql, [regex])
        select_sql = 'DELETE FROM tmp WHERE NOT hash REGEXP ?'
        self.cur.execute(select_sql, [regex])

    def clear_tmp(self):
        deleteTmp_sql = "DELETE from tmp"
        self.cur.execute(deleteTmp_sql)

    def execute(self, sql):
        return self.cur.execute(sql)

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()
