import pathlib

from pydupe.db import PydupeDB
import pydupe.hasher

def clean(dbname: pathlib.Path) -> None:
    if list_of_files_to_update := pydupe.hasher.get_dupes_where_hash_is_NULL(dbname):

        files_not_on_disk = [pathlib.Path(x) for x in list_of_files_to_update if not pathlib.Path(x).is_file()]
        if files_not_on_disk:
            with PydupeDB(dbname) as db:
                for file in files_not_on_disk:
                    db.delete_file(file)
                db.commit()

        pydupe.hasher.rehash_dupes_where_hash_is_NULL(dbname,
            list_of_files_to_update=list_of_files_to_update)
