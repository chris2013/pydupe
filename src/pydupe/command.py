import pathlib

from pydupe.db import PydupeDB
from pydupe.hasher import Hasher


def clean(hsr: Hasher) -> None:
    if list_of_files_to_update := hsr.get_dupes_where_hash_is_NULL():

        files_not_on_disk = [pathlib.Path(x) for x in list_of_files_to_update if not pathlib.Path(x).is_file()]
        if files_not_on_disk:
            with PydupeDB(hsr._dbname) as db:
                for file in files_not_on_disk:
                    db.delete_file(file)
                db.commit()

        hsr.rehash_dupes_where_hash_is_NULL(
            list_of_files_to_update=list_of_files_to_update)
