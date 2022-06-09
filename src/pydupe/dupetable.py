import copy
import itertools
import logging
import pathlib as pl
import re
import shutil
import sys
import typing as tp
import shutil

from more_itertools import chunked
from rich.logging import RichHandler
from rich.progress import Progress
from rich.table import Table
from rich.tree import Tree

from pydupe.config import cnf
from pydupe.console import console
from pydupe.db import PydupeDB
from pydupe.lutable import LuTable
from pydupe.utils import mytimer

FORMAT = "%(message)s"
logging.basicConfig(level=cnf['LOGLEVEL'], format=FORMAT, datefmt="[%X]", handlers=[
                    RichHandler(show_level=True, show_path=True, markup=True, console=console)])
log = logging.getLogger(__name__)


class Error(Exception):
    """Base class for exceptions in this module."""


class DupeFileNotFound(Error):
    """File not found."""


class DupeIsSymLink(Error):
    """Symbolic Link detected."""


class DupeIsDirectory(Error):
    """Directory detected instead of File."""


class DupeArgumentMissing(Error):
    """Argument missing."""


class DupeNotValidated(Error):
    """Argument missing."""


def rename_if_exists(file: pl.Path) -> pl.Path:
    # rename if file exists

    if file.exists():
        numb = 1
        file_stem = file.stem
        while True:
            newPath = file.with_stem(file_stem + "_" + str(numb))
            if newPath.exists():
                numb += 1
            else:
                return newPath
    return file

def move_file_to_trash(*, file: pl.Path, trash: str) -> str:
    assert isinstance(file, pl.Path)

    if cnf['SYSTEM'] == 'Windows':
        addpath = list(file.parts[1:])
        target = pl.Path(trash)
        for f in addpath:
            target = target.joinpath(f)
    else:
        target = pl.Path(str(trash) + str(file))

    target = rename_if_exists(target)

    # this was an issue with freebsd during moving across filesystems
    # try:
    #     file.rename(target)
    # except OSError as e:
    #     if e.errno == 18:
    #         try:
    #             copy_file(source=file, target=target)
    #         except:
    #             raise
    #         else:
    #             delete_file(file=file, trash="DELETE")
    #     else:
    #         raise
    if trash == 'DELETE':
        file.unlink()
    else:
        if not target.parent.is_dir():
            target.parent.mkdir(parents=True)
        shutil.move(src = file, dst = target)

    return str(file)


def is_relative_to(parent: pl.Path, testfile: pl.Path) -> bool:
    """
    this is basically the is_relative_to function from pl available in python 3.9
    """
    assert isinstance(parent, pl.Path)
    assert isinstance(testfile, pl.Path)

    if sys.version_info.major >= 3 and sys.version_info.minor >= 9:
        return testfile.is_relative_to(parent)
    else:
        cmp = [x == parent for x in testfile.parents]
        return any(cmp)


def check_and_autoselect(*, deltable: LuTable[str, pl.Path], keeptable: LuTable[str, pl.Path], autoselect_pattern: str = ".") -> tuple[LuTable[str, pl.Path], LuTable[str, pl.Path]]:
    """
    autoselect filters items that are contained in deltable (marked for deletion) and
    at the same time are not contained in keeptable. This is to avoid a deletion of all
    dupes belonging to one hash. If all items are marked for deletion, pattern matches the first
    file that should be deleted.
    """

    prog = re.compile(autoselect_pattern)
    old_deltable: LuTable[str, pl.Path] = copy.deepcopy(deltable)

    for hsh in sorted(old_deltable.keys()):
        if hsh not in keeptable.keys():
            # all files will be deleted -> move everything to keeptable
            keeptable.lextend(deltable, hsh)
            deltable.ldel([hsh])
            # now check, if one of these files should be deleted nevertheless
            values = sorted(old_deltable[hsh])
            for f in values:
                fname = f.name
                if prog.search(fname):
                    keeptable.discard((hsh, f))
                    assert len(keeptable[hsh]) > 0
                    deltable.add((hsh, f))
                    break

    return deltable, keeptable


def get_dupes(dbname: pl.Path = pl.Path.home() / ".pydupe.sqlite") -> LuTable[str, pl.Path]:
    hashlu: LuTable[str, pl.Path] = LuTable()
    with PydupeDB(dbname) as db:
        for row in db.get_dupes():
            file_as_path = pl.Path(row['filename'])
            hsh: str = row['hash']
            hashlu.add((hsh, file_as_path))
    return hashlu


def dd3(dupes: LuTable[str, pl.Path], *, deldir: str, pattern: str, match_deletions: bool = True, dupes_global: bool = False, autoselect: bool = False) -> tp.Tuple[LuTable[str, pl.Path], LuTable[str, pl.Path]]:
    """
    identify dupes within <deldir> to delete based on <pattern> matching.
    match_deletions: if True (default), matches will be marked for deletion otherwise non-matches will be marked.
    dupes_global: if False(default), at least one dupe will be preserved within deldir,
                        if True, all dupes within deldir will be deleted if at least on dupe exists outside deldir.
    autoselect: if dupes_global is True and no dupe exists outside deldir, autoselect dupes within deldir.
    """
    # deldir ist the Directory to investigate

    # dir_hashlu and outside_hashlu separate dupes in deldir from dupes outside deldir
    dir_hashlu: LuTable[str, pl.Path] = LuTable()
    outside_hashlu: LuTable[str, pl.Path] = LuTable()
    t: mytimer = mytimer()

    for hsh, f in dupes:
        # if pl.Path(f).is_relative_to(deldir): needs Python 3.9 ...
        if is_relative_to(parent=pl.Path(deldir), testfile=f):
            dir_hashlu.add((hsh, f))
        else:
            outside_hashlu.add((hsh, f))
    log.debug("done: partition according to "+str(deldir)+" "+t.get)

    # delete from outside_hashlu dupes that are not also in dir_hashlu
    delitems = set(outside_hashlu.keys())-set(dir_hashlu.keys())
    outside_hashlu.ldel(delitems)

    # match_hashlu and no_match_hashlu contain matches/no-matches to pattern for files within deldir
    match_hashlu: LuTable[str, pl.Path] = LuTable()
    no_match_hashlu: LuTable[str, pl.Path] = LuTable()

    prog = re.compile(pattern)

    for hash, f in dir_hashlu:
        if prog.search(f.name):
            match_hashlu.add((hash, f))
        else:
            no_match_hashlu.add((hash, f))
    log.debug("done: partition matches "+t.get)

    # keeptable and deltable are hash-lookups for files to keep and delete respectively
    keeptable: LuTable[str, pl.Path] = LuTable()
    deltable: LuTable[str, pl.Path] = LuTable()

    # now sort the matches in deltable or keeptable and trat also global dupes
    if match_deletions:
        deltable = match_hashlu
        # keep all not yet specified to be delted.
        keeptable |= no_match_hashlu

        if dupes_global:
            keeptable |= outside_hashlu
        else:
            # need to delete hashes with just one dupe in deltable and no dupe in keeptable.
            # This is because if dupes_local, single dupes (from the global level) should be
            # treated as no dupe if taken just the local scope into account.
            # Application is limited, but a key error is raised later otherwise.
            keys_only_in_deltable = set(
                deltable.keys()) - set(keeptable.keys())
            for key in keys_only_in_deltable:
                if len(deltable[key]) == 1:
                    del deltable[key]

        if autoselect:
            autoselect_pattern = "."  # matches anything
        else:
            autoselect_pattern = "a^"  # matches nothing

        deltable, keeptable = check_and_autoselect(
            deltable=deltable, keeptable=keeptable, autoselect_pattern=autoselect_pattern)

    else:
        keeptable = match_hashlu
        # delete all not yet specified to be kept
        deltable |= no_match_hashlu

        if dupes_global:
            deltable |= outside_hashlu
    log.debug("done: separated into deltable and keeptable "+t.get)
    return deltable, keeptable


class Dupes:

    def __init__(self, dbname: pl.Path = pl.Path.home() / ".pydupe.sqlite") -> None:
        self.dupes = get_dupes(dbname)

    def get_dir_counter(self) -> tp.Counter[str]:
        alldupes = self.dupes.chain_values()
        dir_counter: tp.Counter[str] = tp.Counter()
        for x in alldupes:
            dir_counter.update({str(x.parent): 1})
        return dir_counter

    def print_most_common(self, depth: int) -> None:
        table = Table()
        table.add_column("Directory", justify="right",
                         style="cyan", no_wrap=True)
        table.add_column("# of dupes", justify="left", style="green")

        if len(self.dupes):
            for f, c in self.get_dir_counter().most_common(depth):
                table.add_row(f, str(c))
            console.print(table)
        else:
            console.print("[red]no dupes found")


class Dupetable(Dupes):
    """an instance of this class represents a processed set of dupes separated in files to keep (keeptable)
    and files to delete (deltable). To handle just dupes without separating into keeptable and deltable, this separation is
    postprocessed by method dedupe. if _deduped is True, the postprocessing was done and keeptable and deltable are available.
    These tables can be extracted by method get_deltable and get_keeptable. Automatic postprocessing can be enabled by the flag dedupe."""

    def __init__(self, *, deldir: str, pattern: str, match_deletions: bool = True, dupes_global: bool = False, autoselect: bool = False, dbname: pl.Path = pl.Path.home() / ".pydupe.sqlite", dedupe: bool = False) -> None:
        with Progress(console=console) as progress:
            task_dedupe = progress.add_task("[green]deduping...", start=False)

            super().__init__(dbname=dbname)
            self._deduped: bool = False
            self._dbname: pl.Path = dbname
            self._deldir: str = deldir
            self._pattern: str = pattern
            self._match_deletions: bool = match_deletions
            self._dupes_global: bool = dupes_global
            self._autoselect: bool = autoselect
            self._keeptable: LuTable[str, pl.Path] = LuTable()
            self._deltable: LuTable[str, pl.Path] = LuTable()

            if dedupe:
                log.debug("start deduping")
                self.dedupe()

            progress.update(task_dedupe, advance=1)

    def dedupe(self) -> None:

        self._deltable, self._keeptable = dd3(
            self.dupes, deldir=self._deldir, pattern=self._pattern, match_deletions=self._match_deletions, dupes_global=self._dupes_global, autoselect=self._autoselect)
        self._deduped = True

    def get_deltable(self) -> LuTable[str, pl.Path]:
        return self._deltable

    def get_keeptable(self) -> LuTable[str, pl.Path]:
        return self._keeptable

    def print_tree(self, outfile: pl.Path) -> None:
        outfile = outfile.resolve()
        outfile_st: str = str(outfile)
        console.record = True

        if len(self._keeptable) + len(self._deltable):
            console.print("[red]deletions: "+str(len(self._deltable)) +
                          " [green]keeps: "+str(len(self._keeptable)))
            dupestree = Tree(
                "[bold]Dupes Tree [not bold red] red: dupes to be deleted [green] green: dupes to keep")

            for hash in self._deltable.keys() | self._keeptable.keys():
                branch = dupestree.add(hash[:10] + "...")

                if hash in self._deltable.keys():
                    for delfile in self._deltable[hash]:
                        branch.add("[red]" + str(delfile))
                if hash in self._keeptable.keys():
                    for keepfile in self._keeptable[hash]:
                        branch.add("[green]" + str(keepfile))

            console.print(dupestree)

            if outfile:
                console.save_html(outfile_st)

            console.record = False

        else:
            console.print("[red]no dupes flagged for deletion or to be kept")

    def validate(self) -> None:

        common = self._keeptable.keys() & self._deltable.keys()

        # test for files in both tables
        # to safe memory, iterate over common keys
        for key in common:
            set_keep = set(self._keeptable[key])
            set_delete = set(self._deltable[key])
            if not set_keep & set_delete == set():
                raise DupeNotValidated(
                    "File selected for deletion and to be kept at the same time: ", str(set_keep & set_delete))

        # test for existance, not a directory and not a symlink
        for file in itertools.chain(self._keeptable.chain_values(), self._deltable.chain_values()):
            if not file.exists():
                raise DupeFileNotFound(str(file))
            elif file.is_dir():
                raise DupeIsDirectory(str(file))
            elif file.is_symlink():
                raise DupeIsSymLink(str(file))

    def delete(self, trash: str) -> None:

        if not (len(self._deltable) or self._deduped):
            console.print("[red]no dupes to delete")
            return

        with Progress(console=console) as progress:
            task_validate = progress.add_task(
                "[green]validating dupes ...", total=1)
            self.validate()
            progress.update(task_validate, advance=1)

        filelist_chunked = list(
            chunked(self._deltable.chain_values(), 20))

        with Progress(console=console) as progress:
            task_move_file_to_trash = progress.add_task(
                "[red]moving files to trash ...", total=len(filelist_chunked))

            for chunk in filelist_chunked:
                with PydupeDB(self._dbname) as db:
                    for delfile in chunk:
                        console.print(move_file_to_trash(
                            file=delfile, trash=trash))
                        db.delete_file(filename=delfile)
                    db.commit()

                progress.update(task_move_file_to_trash, advance=1)

            console.print("[green]moved " +
                          str(len(self._deltable)) + " files\n")
