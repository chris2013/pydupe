import concurrent.futures
import logging
from pathlib import Path as p
from re import I
import subprocess
import typing as tp

from more_itertools import chunked
from rich.logging import RichHandler
from rich.progress import Progress

from pydupe.config import cnf
from pydupe.console import console, spinner
from pydupe.db import PydupeDB
from pydupe.utils import mytimer
from pydupe.data import fparms, from_path

FORMAT = "%(message)s"
logging.basicConfig(level=cnf['LOGLEVEL'], format=FORMAT, datefmt="[%X]", handlers=[
                    RichHandler(show_level=True, show_path=True, markup=True, console=console)])
log = logging.getLogger(__name__)


def hash_file(file: str) -> str:
    cmd: list[str] = cnf['HASHEXECUTE_1'] + [file]

    sub = subprocess.Popen(
        cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = sub.communicate()
    if stderr:
        log.error("Errorcode: "+stderr+" while hashing "+file)
    # hash ist ASCII, therefore no decode to UTF-8 necessary
    if cnf['SYSTEM'] == 'Windows':
        hsh = stdout.splitlines()[1]
    else:
        hsh = stdout[0:64]

    return hsh

@spinner(console, "scan files on disk")
def scan_files_on_disk_and_insert_stats_in_db(dbname: p, path: p) -> int:
    assert isinstance(path, p), 'must be of type Pathlib.Path'
    assert path.is_absolute, 'path must be absolute'

    with PydupeDB(dbname) as db:
        filelist = list(path.rglob("*"))
        list_of_fparms: list[fparms] = []
        for item in filelist:
            if item.is_file() and not item.is_symlink():    # only files and no symlink make it into database
                if "/." in str(item):
                    pass                                    # do not recurse hidden dirs and hidden files
                else:
                    list_of_fparms.append(from_path(item))
        db.parms_insert(list_of_fparms)
        db.commit()

    return len(list_of_fparms)


def rehash_dupes_where_hash_is_NULL(dbname: p) -> int:

    list_of_files_to_update = PydupeDB(
        dbname).get_list_of_equal_sized_files_where_hash_is_NULL()

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


def clean(dbname: p) -> None:
    """ this deletes files in table lookup that are not on disk anymore but would be tried to get rehashed """

    list_of_files_to_update = PydupeDB(
        dbname).get_list_of_equal_sized_files_where_hash_is_NULL()

    with PydupeDB(dbname) as db:
        for file in (p(x) for x in list_of_files_to_update if not p(x).is_file()):
            db.delete_file_lookup(file)
        db.commit()
