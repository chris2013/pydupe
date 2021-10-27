import itertools
import pathlib
import shutil

from rich.progress import Progress
from rich.tree import Tree

from pydupe.console import console
from pydupe.db import PydupeDB


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


class DupeHashNotValidated(Error):
    """Argument missing."""


def copy_file(*, source: pathlib.Path, target: pathlib.Path) -> None:
    assert source.is_file()
    # str() only there for Python < (3, 6)
    shutil.copy(str(source), str(target))


def move_file_to_trash(*, file: pathlib.Path, trash: str) -> str:
    assert isinstance(file, pathlib.Path)
    target = pathlib.Path(trash + str(file))
    if not target.parent.is_dir():
        target.parent.mkdir(parents=True)

    try:
        file.rename(target)
    except OSError as e:
        if e.errno == 18:
            console.print(
                "[red]copying instead of renaming, put trash on the same filesystem to be faster!")
            try:
                copy_file(source=file, target=target)
            except:
                raise
            else:
                delete_file(file=file, trash="DELETE")
    else:
        raise

    return str(target)


def delete_file(*, file: pathlib.Path, trash: str) -> None:
    assert isinstance(file, pathlib.Path)
    assert trash == "DELETE"
    file.unlink()


def validator(table: dict) -> None:
    for _, flist in table.items():
        for f in flist:
            if not f.exists():
                raise DupeFileNotFound(str(f))
            elif f.is_dir():
                raise DupeIsDirectory(str(f))
            elif f.is_symlink():
                raise DupeIsSymLink(str(f))


def print_dupestree(*, deltable: dict, keeptable: dict, outfile: str):
    hashes_of_files_to_delete = deltable.keys()
    num_keeps = sum([len(x) for x in keeptable.values()])
    num_deletes = sum([len(x) for x in deltable.values()])
    console.print("[red]deletions: "+str(num_deletes) +
                  " [green]keeps: "+str(num_keeps))
    dupestree = Tree(
        "Dupes Tree [red] red: dupes to be deleted [green] green: dupes to keep")
    for hash in hashes_of_files_to_delete:
        branch = dupestree.add(hash[:5] + "...")
        for delfile in deltable[hash]:
            branch.add("[red]" + str(delfile))
        for keepfile in keeptable[hash]:
            branch.add("[green]" + str(keepfile))
    console.print(dupestree)
    console.save_html(outfile)


def do_move(dbname, *, deltable: dict, trash: str):
    with Progress(console=console) as progress:
        hashes_of_files_to_delete = deltable.keys()
        files_to_delete = list(
            itertools.chain.from_iterable(deltable.values()))

        if trash == "DELETE":
            task_move_file_to_trash = progress.add_task(
                "[red]deleting files ...", total=len(files_to_delete))
        else:
            task_move_file_to_trash = progress.add_task(
                "[red]moving files to trash ...", total=len(files_to_delete))
        with PydupeDB(dbname=dbname) as db:
            for hash in hashes_of_files_to_delete:
                for delfile in deltable[hash]:
                    if trash == "DELETE":
                        console.print(str(delfile))
                        delete_file(file=delfile, trash=trash)
                    else:
                        console.print(move_file_to_trash(
                            file=delfile, trash=trash))
                    db.delete_file(filename=str(delfile))
                    db.commit()
                    progress.update(task_move_file_to_trash, advance=1)
