import concurrent.futures
import pathlib
import platform
import subprocess

from more_itertools import chunked
from rich.progress import Progress

from pydupe.console import console
from pydupe.db import PydupeDB


class Hasher():
    def __init__(self, dbname=str(pathlib.Path.home()) + "/" + ".pydupe.sqlite"):
        self._dbname = dbname
        if (systm := platform.system()) == 'Linux':
            self.HASHEXECUTE = ['sha256sum']
        elif systm == 'FreeBSD':
            self.HASHEXECUTE = ['shasum', '-a', '256']

        self._platform = platform.system()
        self.rehash_rows_where_hash_is_NULL()

    def move_dbcontent_for_dir_to_tmp(self, path: str):
        with PydupeDB(self._dbname) as db:
            db.copy_dir_to_table_tmp(path)
            db.delete_dir(path)
            db.commit()

    def scan_files_on_disk_and_insert_stats_in_db(self, path: str):
        with PydupeDB(self._dbname) as db:
            with Progress(console=console) as progress:
                task = progress.add_task(
                    "[red] get file statistics ...", start=False)
                filelist = list(pathlib.Path(path).rglob("*"))
                progress.update(task, total=len(filelist))
                for item in filelist:
                    progress.update(task, advance=1)
                    if item.is_file() and not item.is_symlink(): # only files and no symlink make it into database
                        if "/." in (item_str := str(item)):
                            pass  # do not recurse hidden dirs and hidden files
                        else:
                            size, inode, mtime, ctime = self.get_stats_of_file(
                                item)
                            db.insert(
                                (item_str, None, size, inode, mtime, ctime))
            db.commit()

    def copy_hash_from_tmp_if_unchanged_inode_size_mtime_ctime(self):
        with PydupeDB(self._dbname) as db:
            db.copy_hash_to_table_lookup()
            db.commit()
            db.clear_tmp()
            db.commit()

    def rehash_rows_where_hash_is_NULL(self):
        with PydupeDB(self._dbname) as db:
            list_of_files_to_update = db.get_list_of_files_where_hash_is_NULL()
        filelist_chunked = list(chunked(list_of_files_to_update, 200))

        def hash_file(file):
            sub = subprocess.Popen(
                self.HASHEXECUTE + [file], text=True, stdout=subprocess.PIPE)
            console.print(file)
            return file, sub.stdout.read()[0:64]

        with Progress(console=console) as progress:
            if count:=len(filelist_chunked):
                task_commit = progress.add_task(
                    "[green] committing to sqlite ...", total=len(filelist_chunked))
            for chunk in filelist_chunked:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    result = executor.map(hash_file, chunk)
                with PydupeDB(self._dbname) as db:
                    for file, hash in result:
                        db.update_hash(filename=file, hash=hash)
                    db.commit()
                    progress.update(task_commit, advance=1)

    def hashdir(self, path: str):
        self.move_dbcontent_for_dir_to_tmp(path)
        self.scan_files_on_disk_and_insert_stats_in_db(path)
        self.copy_hash_from_tmp_if_unchanged_inode_size_mtime_ctime()
        self.rehash_rows_where_hash_is_NULL()

    @staticmethod
    def get_stats_of_file(item: pathlib.Path) -> tuple:
        mode, inode, dev, nlink, uid, gid, size, atime, mtime, ctime = item.stat()
        return size, inode, mtime, ctime
