import concurrent.futures
import logging
import pathlib
import subprocess
import typing as tp

from more_itertools import chunked
from rich.logging import RichHandler
from rich.progress import Progress

from pydupe.config import cnf
from pydupe.console import console
from pydupe.db import PydupeDB
from pydupe.utils import mytimer
from pydupe.data import fparms, from_path

FORMAT = "%(message)s"
logging.basicConfig(level=cnf['LOGLEVEL'] , format=FORMAT, datefmt="[%X]", handlers=[
                    RichHandler(show_level=True, show_path=True, markup=True, console=console)])
log = logging.getLogger(__name__)

def hash_file(file: str) -> str:
    cmd: tp.List[str] = cnf['HASHEXECUTE_1'] + [file]
    if cnf['HASHEXECUTE_2']:
        cmd += cnf['HASHEXECUTE_2']

    sub = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = sub.communicate()
    if stderr:
        log.error("Errorcode: "+stderr+" while hashing "+file) 
    # hash ist ASCII, therefore no decode to UTF-8 necessary
    if cnf['SYSTEM'] == 'Windows':
        hsh = stdout.splitlines()[1]
    else:
        hsh = stdout[0:64]

    return hsh

def scan_files_on_disk_and_insert_stats_in_db(dbname: pathlib.Path, path: pathlib.Path) -> int:
    assert isinstance(path, pathlib.Path)
    i = 0

    with PydupeDB(dbname) as db:
        with Progress(console=console) as progress:
            task = progress.add_task(
                "[green] get file statistics ...", start=False)
            filelist = list(path.rglob("*"))
            progress.update(task, total=len(filelist))
            list_of_fparms: list[fparms] = []
            for item in filelist:
                progress.update(task, advance=1)
                if item.is_file() and not item.is_symlink():    # only files and no symlink make it into database
                    if "/." in (item_str := str(item)):
                        pass                                    # do not recurse hidden dirs and hidden files
                    else:
                        i += 1
                        list_of_fparms.append(from_path(item))
            db.parms_insert(list_of_fparms)
        db.commit()

    return i

def rehash_dupes_where_hash_is_NULL(dbname: pathlib.Path, list_of_files_to_update: tp.Optional[tp.List[str]]=None) -> int:

    if not list_of_files_to_update:
        list_of_files_to_update = PydupeDB(dbname).get_list_of_equal_sized_files_where_hash_is_NULL()

    filelist_chunked = list(chunked(list_of_files_to_update, 1000))

    with Progress(console=console, auto_refresh=False) as progress:
        if count := len(filelist_chunked):
            task_commit = progress.add_task(
                "[green] hashing and committing to sqlite ...", total=count)

        for chunk in filelist_chunked:

            with concurrent.futures.ThreadPoolExecutor() as executor:
                to_do_map = {}
                for file in chunk:
                    future = executor.submit(hash_file, file)
                    to_do_map[future] = file
                done_iter = concurrent.futures.as_completed(to_do_map)

            with PydupeDB(dbname) as db:
                hashlist: list[tuple[tp.Optional[str], str]] = []
                for future in done_iter:
                    file = to_do_map[future]
                    try:
                        hash = future.result()
                    except Exception as exc:
                        raise OSError("exception processing file {file}")

                    hashlist.append((hash, file))
                db.update_hash(hashlist)
                db.commit()

                progress.update(task_commit, advance=1)
                progress.refresh()

    return len(list_of_files_to_update)

def hashdir(dbname: pathlib.Path, path: pathlib.Path) -> tp.Tuple[int, int]:
    t = mytimer()
    log.debug("started: move dbcontent for dir to permanent")
    PydupeDB(dbname).move_dbcontent_for_dir_to_permanent(path)
    log.debug("done: move dbcontent for dir to permanent "+t.get)
    log.debug("started: scan files on disk and insert stats in db") 
    number_scanned = scan_files_on_disk_and_insert_stats_in_db(dbname, path)
    log.debug("done: scan files on disk and insert stats in db "+t.get)
    log.debug("start: copy hash from permanent if unchanged stats")
    PydupeDB(dbname).copy_hash_from_permanent_if_unchanged_inode_size_mtime_ctime()
    log.debug("done: copy hash from permanent if unchanged stats "+t.get)
    log.debug("started: rehash_dupes_where_hash_is_NULL")
    number_hashed = rehash_dupes_where_hash_is_NULL(dbname)
    log.debug("done: rehash_dupes_where_hash_is_NULL "+t.get)
    
    return number_scanned, number_hashed

def clean(dbname: pathlib.Path) -> None:

    if list_of_files_to_update := PydupeDB(dbname).get_list_of_equal_sized_files_where_hash_is_NULL():

        with PydupeDB(dbname) as db:
            for file in (pathlib.Path(x) for x in list_of_files_to_update if not pathlib.Path(x).is_file()):
                db.delete_file(file)
            db.commit()

        rehash_dupes_where_hash_is_NULL(dbname,
            list_of_files_to_update=list_of_files_to_update)