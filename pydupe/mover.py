import itertools
import pathlib

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



class Mover():

    def __init__(self, dbname=str(pathlib.Path.home()) + "/" + ".pydupe.sqlite"):
        self._dbname = dbname

    @staticmethod
    def move_file_to_trash(*, filename: str, trash: str) -> str:
        source = pathlib.Path(filename)
        target = pathlib.Path(trash + str(source))
        if not target.parent.is_dir():
            target.parent.mkdir(parents=True)
        return str(source.rename(target))

    @staticmethod
    def validator(table: dict) -> None:
        for _, flist in table.items():
            for f in flist:
                p = pathlib.Path(f)
                if not p.exists():
                    raise DupeFileNotFound(str(f))
                elif p.is_dir():
                    raise DupeIsDirectory(str(f))
                elif p.is_symlink():
                    raise DupeIsSymLink(str(f))

    @staticmethod
    def print_dupestree(*, deltable: dict, keeptable: dict, outfile: str):
        hashes_of_files_to_delete = deltable.keys()
        dupestree = Tree(
            "Dupes Tree [red] red: dupes to be deleted [green] green: dupes to keep")
        for hash in hashes_of_files_to_delete:
            branch = dupestree.add(hash[:5] + "...")
            for delfile in deltable[hash]:
                branch.add("[red]" + delfile)
            for keepfile in keeptable[hash]:
                branch.add("[green]" + keepfile)
        console.print(dupestree)
        console.save_html(outfile)

    def do_move(self, *, deltable: dict, trash: str):
        with Progress(console=console) as progress:
            hashes_of_files_to_delete = deltable.keys()
            files_to_delete = list(
                itertools.chain.from_iterable(deltable.values()))

            task_move_file_to_trash = progress.add_task(
                "[red]moving files to trash ...", total=len(files_to_delete))
            with PydupeDB(dbname=self._dbname) as db:
                for hash in hashes_of_files_to_delete:
                    for delfile in deltable[hash]:
                        console.print(self.move_file_to_trash(
                            filename=delfile, trash=trash))
                        db.delete_file(filename=delfile)
                        db.commit()
                        progress.update(task_move_file_to_trash, advance=1)
