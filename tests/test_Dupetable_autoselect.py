import os
from pathlib import Path as p
import tempfile
import typing as tp

import pydupe.dupetable as dupetable
import pydupe.hasher
import pytest
from click.testing import CliRunner
from pydupe.cli import cli

cwd = str(p.cwd())
tdata = cwd + "/pydupe/pydupe/"
home = str(p.home())


@pytest.fixture
def setup_database() -> tp.Generator[tuple[p, dupetable.Dupetable],None , None]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        cwd = p.cwd()
        dbname = p(newpath) / ".pydupe.sqlite"
        file_exists = cwd  / "file_exists"
        file_is_dupe = cwd / "somedir" / "file_is_dupe"
        dupe2_in_dir = cwd / "somedir" / "somedir2" / "dupe2_in_dir"
        dupe_in_dir = cwd / "somedir" / "somedir2" / "dupe_in_dir"
        
        dupe2_in_dir.parent.mkdir(parents=True)
        dupe2_in_dir.write_text("some dummy text") 
        file_exists.write_text("some dummy text") 
        file_is_dupe.write_text("some dummy text") 
        dupe_in_dir.write_text("some dummy text") 
        
        runner = CliRunner()
        runner.invoke(cli, ['--dbname', str(dbname), 'hash', str(cwd)])

        deldir = p.cwd() / "somedir"
        pattern = "_dupe"
        match_deletions = True
        dupes_global = True
        autoselect = False
        dedupe = True

        Dt = dupetable.Dupetable(dbname=dbname, deldir=deldir, pattern = pattern, match_deletions=match_deletions, dupes_global=dupes_global, autoselect=autoselect, dedupe=dedupe)

        yield cwd, Dt

        os.chdir(old_cwd)


class TestDupetable:

    def test_Dupetable_deletions_global_pattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        
        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
                {p/'somedir/file_is_dupe'}}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'file_exists', p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }

    def test_Dupetable_deletions_local_pattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._dupes_global = False
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
                {p/'somedir/file_is_dupe'}}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }

    def test_Dupetable_deletions_global_nopattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._pattern = "."
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'somedir/file_is_dupe', p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'file_exists'}
        }

    def test_Dupetable_deletions_global2_nopattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._pattern = "."
        Dt._deldir = p
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
            {p/'file_exists', p/'somedir/file_is_dupe',
             p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }

    def test_Dupetable_deletions_local_nopattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._pattern = "."
        Dt._dupes_global = False
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable
        
        assert d.as_dict_of_sets() == {}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'somedir/file_is_dupe', p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }

    def test_Dupetable_keeps_global_pattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._match_deletions = False
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
            {p/'file_exists', p/'somedir/somedir2/dupe2_in_dir',
                p/'somedir/somedir2/dupe_in_dir'}
        }
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
                {p/'somedir/file_is_dupe'}}

    def test_Dupetable_keeps_local_pattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._match_deletions = False
        Dt._dupes_global = False
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
            {p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}}

        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
                {p/'somedir/file_is_dupe'}}

    def test_Dupetable_keeps_global_nopattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._match_deletions = False
        Dt._pattern = "."
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'file_exists'}}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
            {p/'somedir/file_is_dupe', p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}}

    def test_Dupetable_keeps_global2_nopattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._match_deletions = False
        Dt._pattern = "."
        Dt._deldir = p
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable

        assert d.as_dict_of_sets() == {}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4':
            {p/'file_exists', p/'somedir/file_is_dupe',
             p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }

    def test_Dupetable_keeps_local_nopattern(self, setup_database: tuple[p, dupetable.Dupetable]) -> None:
        p, Dt = setup_database
        Dt._match_deletions = False
        Dt._dupes_global = False
        Dt._pattern = "."
        Dt.dedupe()

        d = Dt._deltable
        k = Dt._keeptable
        
        assert d.as_dict_of_sets() == {}
        assert k.as_dict_of_sets() == {
            '093d9d18a0d8233e8fadd6a9c4cf4a5c578f9dc1cf64f54589943fdb840ee0a4': {p/'somedir/file_is_dupe', p/'somedir/somedir2/dupe2_in_dir', p/'somedir/somedir2/dupe_in_dir'}
        }
