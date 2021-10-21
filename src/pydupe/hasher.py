import concurrent.futures
import logging
import pathlib
import subprocess

from more_itertools import chunked
from rich.logging import RichHandler
from rich.progress import Progress

from pydupe.config import cnf
from pydupe.console import console
from pydupe.db import PydupeDB

FORMAT = "%(message)s"
logging.basicConfig(level=logging.NOTSET, format=FORMAT, datefmt="[%X]", handlers=[
                    RichHandler(show_level=True, show_path=True, markup=True, console=console)])
log = logging.getLogger(__name__)


def init(dbname=str(pathlib.Path.home()) + "/" + ".pydupe.sqlite"):
    rehash_rows_where_hash_is_NULL(_dbname=dbname)

def hashdir(_dbname, path: str):
    with Progress() as progress:
        log.debug("[red]started: move dbcontent for dir to table tmp")
        move_dbcontent_for_dir_to_tmp(_dbname, path)
        log.debug("[green]finished: move dbcontent for dir to table tmp")

        log.debug("[red]started: scan files on disk and insert stats into db")
        scan_files_on_disk_and_insert_stats_in_db(_dbname, path)
        log.debug("[green]finished: scan files on disk and insert stats into db")

        log.debug("[red]started: copy hash from tmp if available")
        copy_hash_from_tmp_if_unchanged_inode_size_mtime_ctime(_dbname)
        log.debug("[green]finished: copy hash from tmp if available")

    log.debug("[red]started: rehash rows where hash is NULL")
    number_scanned = rehash_rows_where_hash_is_NULL(_dbname)
    log.debug("[green]finished: rehash rows where hash is NULL")
    return number_scanned


def move_dbcontent_for_dir_to_tmp(_dbname, path: str):
    with PydupeDB(_dbname) as db:
        log.debug("[red]started: copy_dir_to_table_tmp")
        db.copy_dir_to_table_tmp(path)
        log.debug("[green]finished: copy_dir_to_table_tmp")

        log.debug("[red]started: delete dir")
        db.delete_dir(path)
        log.debug("[green]finished: delete dir")
        db.commit()


def scan_files_on_disk_and_insert_stats_in_db(_dbname, path: str):
    with PydupeDB(_dbname) as db:
        with Progress(console=console) as progress:
            task = progress.add_task(
                "[red] get file statistics ...", start=False)
            filelist = list(pathlib.Path(path).rglob("*"))
            progress.update(task, total=len(filelist))
            for item in filelist:
                progress.update(task, advance=1)
                if item.is_file() and not item.is_symlink():  # only files and no symlink make it into database
                    if "/." in (item_str := str(item)):
                        pass  # do not recurse hidden dirs and hidden files
                    else:
                        size, inode, mtime, ctime = get_stats_of_file(
                            item)
                        db.insert(
                            (item_str, None, size, inode, mtime, ctime))
        db.commit()


def copy_hash_from_tmp_if_unchanged_inode_size_mtime_ctime(_dbname):
    with PydupeDB(_dbname) as db:
        db.copy_hash_to_table_lookup()
        db.commit()
        db.clear_tmp()
        db.commit()


def hash_file(file):
    sub = subprocess.Popen(
        cnf['HASHEXECUTE'] + [file], text=True, stdout=subprocess.PIPE)
    return sub.stdout.read()[0:64]


def rehash_rows_where_hash_is_NULL(_dbname):
    with PydupeDB(_dbname) as db:
        list_of_files_to_update = db.get_list_of_equal_sized_files_where_hash_is_NULL()
    filelist_chunked = list(chunked(list_of_files_to_update, 200))

    with Progress(console=console) as progress:
        if count := len(filelist_chunked):
            task_commit = progress.add_task(
                "[green] hashing and committing to sqlite ...", total=count)
        for batch, chunk in enumerate(filelist_chunked):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                to_do_map = {}
                for file in chunk:
                    future = executor.submit(hash_file, file)
                    to_do_map[future] = file
                done_iter = concurrent.futures.as_completed(to_do_map)

            with PydupeDB(_dbname) as db:
                for future in done_iter:
                    file = to_do_map[future]
                    try:
                        hash = future.result()
                    except Exception as exc:
                        log.critical(f"[red] exception processing file {file}")
                    db.update_hash(filename=file, hash=hash)
                db.commit()
                progress.update(task_commit, advance=1)
    return len(list_of_files_to_update)

def get_stats_of_file(item: pathlib.Path) -> tuple:
    mode, inode, dev, nlink, uid, gid, size, atime, mtime, ctime = item.stat()
    return size, inode, mtime, ctime
