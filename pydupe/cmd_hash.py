import pathlib
import typing as tp

import pydupe.hasher
from pydupe.console import console
from pydupe.db import PydupeDB
from pydupe.utils import mytimer


def cmd_hash(dbname: pathlib.Path, path: pathlib.Path) -> None:
    assert isinstance(path, pathlib.Path), 'must be of type Pathlib.Path'

    t = mytimer() 
    pydupe.hasher.clean(dbname)
    with PydupeDB(dbname) as db:
        db.delete_dir(path)
        db.commit()
    number_scanned = pydupe.hasher.scan_files_on_disk_and_insert_stats_in_db(dbname, path)
    with PydupeDB(dbname) as db:
        db.copy_hash_to_table_lookup()
        db.commit()
    number_hashed = pydupe.hasher.rehash_dupes_where_hash_is_NULL(dbname)
    with PydupeDB(dbname) as db:
        db.copy_dir_to_table_permanent(path)
        db.commit()

    console.print(
        f"[green] scanned {number_scanned} and hashed thereof {number_hashed} files in {t.get} sec")

# if __name__ == "__main__":
#    cmd_hash(dbname=str(pathlib.Path.home()) + '/.pydupe.sqlite', path = pathlib.Path.cwd())