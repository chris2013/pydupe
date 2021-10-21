import collections
import copy
import itertools
import logging
import pathlib
import re

from rich.logging import RichHandler

from pydupe.db import PydupeDB
from pydupe.lutable import LuTable

log = logging.getLogger(__name__)


def iter_lu_table(table):
    for hash in table.keys():
        for file in table[hash]:
            yield hash, file

def is_relative_to(parent: pathlib.Path, testfile: pathlib.Path) -> bool:
    """
    this is basically is_relative_to function from pathlib available in python 3.9
    """
    assert isinstance(parent, pathlib.Path)
    assert isinstance(testfile, pathlib.Path)
    cmp = [x == parent for x in testfile.parents]
    return any(cmp)

def sanitize_dict(dct: dict) -> dict:
    """ remove empty lists from a dict."""
    return {k: v for k, v in dct.items() if v != collections.deque()}

def assure_purepath(obj) -> pathlib.Path:
    obj_is_str = isinstance(obj, str)
    obj_is_pp = isinstance(obj, pathlib.PurePath)
    assert obj_is_str or obj_is_pp
    if obj_is_str:
        return pathlib.PurePath(obj)
    else:
        return obj

def autoselect_(*, deltable: LuTable, keeptable: LuTable, pattern=".") -> tuple:
    """
    autoselect filters items that are contained in deltable (marked for deletion) and
    at the same time are not contained in keeptable. This is to avoid a deletion of all
    dupes of a hash. If all items are marked for deletion, pattern matches the first
    file that should be deleted.
    """
    # pattern matches deletions
    keys_not_in_keeptable = set(deltable.keys())-set(keeptable.keys())
    prog = re.compile(pattern)
    old_deltable = copy.deepcopy(deltable)
    for hsh in keys_not_in_keeptable:
        matched = False
        for f in old_deltable[hsh]:
            if prog.search(f.name) and not matched:

                # select only first match for deletion
                # select for deletion means not to move it to keeptable

                matched = True
            else:
                deltable.discard((hsh, f))
                keeptable.add((hsh, f))
    return deltable, keeptable

def get_dupes(dbname=str(pathlib.Path.home()) + "/" + ".pydupe.sqlite") -> None:
    hashlu = LuTable()
    with PydupeDB(dbname) as db:
        for row in db.get_dupes():
            file_as_path = pathlib.Path(row['filename'])
            hashlu.add((row['hash'], file_as_path))
    return hashlu

def dd3(hashlu, deldir: str, pattern: str, *, match_deletions=True, dupes_global=False, autoselect=False) -> tuple:
    """
    identify dupes within <deldir> to delete based on <pattern> matching.
    match_deletions: if True (default), matches will be marked for deletion otherwise non-matches will be marked.
    dupes_global: if False(default), at least one dupe will be preserved within deldir,
                        if True, all dupes within deldir will be deleted if at least on dupe exists outside deldir.
    autoselect: if dupes_global is True and no dupe exists outside deldir, autoselect dupes within deldir.
    """
    # deldir ist the Directory to investigate

    # dir_hashlu and outside_hashlu separate dupes in deldir from dupes outside deldir
    dir_hashlu = LuTable()
    outside_hashlu = LuTable()

    log.debug("[red]started partitioning into dir_hashlu and outside_hashlu")
    for hsh, f in iter(hashlu):
        # if pathlib.Path(f).is_relative_to(deldir): needs Python 3.9 ...
        if is_relative_to(parent=pathlib.Path(deldir), testfile=f):
            dir_hashlu.add((hsh, f))
        else:
            outside_hashlu.add((hsh, f))

    log.debug("[green]partitioning finished")

    # delete from outside_hashlu dupes that are not also in dir_hashlu
    log.debug("[red]started deleting from outside_hashlu hashes not needed")
    delitems = set(outside_hashlu.keys())-set(dir_hashlu.keys())
    outside_hashlu.ldel(delitems)
    log.debug("[green]finished from outside_hashlu hashes not needed")

    # match_hashlu and no_match_hashlu contain matches/no-matches to pattern for files within deldir
    match_hashlu = LuTable()
    no_match_hashlu = LuTable()

    prog = re.compile(pattern)

    log.debug("[red]started matching in dir_hashlu")
    for hash, f in iter(dir_hashlu):
        if prog.search(f.name):
            match_hashlu.add((hash, f))
        else:
            no_match_hashlu.add((hash, f))
    log.debug("[green]finished matching in dir_hashlu")

    # keeptable and deltable are hash-lookups for files to keep and delete respectively
    keeptable = LuTable()
    deltable = LuTable()

    # now sort the matches in deltable or keeptable and trat also global dupes
    if match_deletions:
        log.debug("[red]started building deltable, match_deletions = TRUE")
        deltable = match_hashlu

        for hash in deltable.keys():
            if hash in no_match_hashlu.keys():
                keeptable.lextend(no_match_hashlu, hash)

        if dupes_global:
            for hash in deltable.keys():
                if hash in outside_hashlu.keys():
                    keeptable.lextend(outside_hashlu, hash)

        if autoselect:
            autoselect_pattern = "."  # matches anything
        else:
            autoselect_pattern = "a^"  # matches nothing

        deltable, keeptable = autoselect_(
            deltable=deltable, keeptable=keeptable, pattern=autoselect_pattern)
        log.debug("[green]finished building deltable, match_deletions = TRUE")

    else:
        log.debug("[red]started building deltable, match_deletions = FALSE")
        keeptable = match_hashlu

        for hash in keeptable.keys():
            if hash in no_match_hashlu.keys():
                deltable.lextend(no_match_hashlu, hash)

        if dupes_global:
            for hash in keeptable.keys():
                if hash in outside_hashlu.keys():
                    deltable.lextend(outside_hashlu, hash)
        log.debug("[green]finished building deltable, match_deletions = FALSE")
    return deltable, keeptable

def get_dir_counter(hashlu) -> collections.Counter:
    log.debug("[red]started get_dir_counter")
    alldupes = itertools.chain.from_iterable(hashlu.values())
    dir_counter = collections.Counter()
    for x in alldupes:
        dir_counter.update({str(x.parent): 1})
    log.debug("[green]finished get_dir_counter")
    return dir_counter

if __name__ == "__main__":
    hashlu = get_dupes()
    deldir = '/home/chris/ttt'
    pattern = "."
    match_deletions = True
    dupes_global = True
    autoselect = True
    deltable, keeptable = dd3(hashlu,
        deldir, pattern, match_deletions=match_deletions, dupes_global=dupes_global, autoselect=autoselect)
