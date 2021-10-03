import collections
import copy
import itertools
import pathlib
import re

from pydupe.db import PydupeDB


class Dupetable:
    """
    _hashlu: dictionary {hash: list of duplicates with same hash (dupes) (as str obj)}
    """
    
    @staticmethod
    def is_relative_to(parent: pathlib.Path, testfile: pathlib.Path) -> bool:
        """
        this is basically is_relative_to function from pathlib available in python 3.9
        """
        assert isinstance(parent, pathlib.Path)
        assert isinstance(testfile, pathlib.Path)
        cmp = [x == parent for x in testfile.parents]
        return any(cmp)

    @staticmethod
    def sanitize_dict(dct: dict) -> dict:
        """ remove empty lists from a dict."""
        return {k: v for k, v in dct.items() if v != []}

    @staticmethod
    def assure_purepath(obj) -> pathlib.Path:
        obj_is_str = isinstance(obj, str)
        obj_is_pp = isinstance(obj, pathlib.PurePath)
        assert obj_is_str or obj_is_pp
        if obj_is_str:
            return pathlib.PurePath(obj)
        else:
            return obj

    @staticmethod
    def autoselect(*,deltable: dict, keeptable: dict, pattern=".") -> tuple:
        """
        autoselect filters items that are contained in deltable (marked for deletion) and
        at the same time are not contained in keeptable. This is to avoid a deletion of all
        dupes of a hashgroup. If all items are marked for deletion, pattern matches the first
        file that should be deleted.
        """
        # pattern matches deletions
        keys_not_in_keeptable = set(deltable.keys())-set(keeptable.keys())
        prog = re.compile(pattern)
        old_deltable = copy.deepcopy(deltable)
        for hsh in keys_not_in_keeptable:
            matched = False
            for f in old_deltable[hsh]:
                if prog.search(f) and not matched:

                    # select only first match for deletion
                    # select for deletion means not to move it to keeptable

                    matched = True
                else:
                    deltable[hsh].remove(f)
                    keeptable[hsh].append(f)
        return Dupetable.sanitize_dict(deltable), Dupetable.sanitize_dict(keeptable)

    def __init__(self, dbname=str(pathlib.Path.home()) + "/" + ".pydupe.sqlite") -> None:
        self._hashlu = collections.defaultdict(list)
        with PydupeDB(dbname) as db:
            for row in db.get_dupes():
                self._hashlu[row['hash']].append(row['filename'])

    def dd3(self, deldir: str, pattern: str, *, match_deletions=True, dupes_global=False, autoselect=False) -> tuple:
        """
        identify dupes within <deldir> to delete based on <pattern> matching.
        match_deletions: if True (default), matches will be marked for deletion otherwise non-matches will be marked.
        dupes_global: if False(default), at least one dupe will be preserved within deldir,
                          if True, all dupes within deldir will be deleted if at least on dupe exists outside deldir.
        autoselect: if dupes_global is True and no dupe exists outside deldir, autoselect dupes within deldir.
        """
        # this is the directory to investigate
        deldir = self.assure_purepath(deldir)

        # dir_hashlu and outside_hashlu separate dupes in deldir from dupes outside deldir
        dir_hashlu = collections.defaultdict(list)
        outside_hashlu = collections.defaultdict(list)

        for hsh, flist in self._hashlu.items():
            for f in flist:
                #if pathlib.Path(f).is_relative_to(deldir): needs Python 3.9 ...
                if self.is_relative_to(parent=pathlib.Path(deldir), testfile=pathlib.Path(f)):
                    dir_hashlu[hsh].append(f)
                else:
                    outside_hashlu[hsh].append(f)

        # delete from outside_hashlu dupes that are not also in dir_hashlu
        delitems = set(outside_hashlu.keys())-set(dir_hashlu.keys())
        for k in delitems:
            outside_hashlu.pop(k)

        # match_hashlu and no_match_hashlu contain matches/no-matches to pattern for files within deldir
        match_hashlu = collections.defaultdict(list)
        no_match_hashlu = collections.defaultdict(list)

        prog = re.compile(pattern)

        for hash, flist in dir_hashlu.items():
            for f in flist:
                if prog.search(f):
                    match_hashlu[hash].append(f)
                else:
                    no_match_hashlu[hash].append(f)

        # keeptable and deltable are hash-lookups for files to keep and delete respectively
        keeptable = collections.defaultdict(list)
        deltable = collections.defaultdict(list)

        # now sort the matches in deltable or keeptable and trat also global dupes
        if match_deletions:
            deltable = match_hashlu

            for hash in deltable.keys():
                if hash in no_match_hashlu.keys():
                    keeptable[hash] += no_match_hashlu[hash]

            if dupes_global:
                for hash in deltable.keys():
                    if hash in outside_hashlu.keys():
                        keeptable[hash] += outside_hashlu[hash]

            if autoselect:
                pattern = "."  # matches anything
            else:
                pattern = "a^"  # matches nothing

            deltable, keeptable = self.autoselect(deltable=deltable, keeptable=keeptable, pattern=pattern)

        else:
            keeptable = match_hashlu

            for hash in keeptable.keys():
                if hash in no_match_hashlu.items():
                    deltable[hash] += no_match_hashlu[hash]

            if dupes_global:
                for hash in keeptable.keys():
                    if hash in outside_hashlu.keys():
                        deltable[hash] += outside_hashlu[hash]

        return deltable, keeptable

    def get_dir_counter(self) -> collections.Counter:
        alldupes = itertools.chain.from_iterable(self._hashlu.values())
        dir_counter = collections.Counter()
        for x in alldupes:
            dir_counter.update({str(pathlib.Path(x).parent): 1})
        return dir_counter
