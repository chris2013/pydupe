import pathlib
import tempfile

import pytest
from click.testing import CliRunner
from pydupe.mover import (DupeFileNotFound, DupeIsDirectory, DupeIsSymLink,
                          Error)
import pydupe.mover as mover

import pydupe.dupetable as dupetable
from pydupe.cli import cli
import os


@pytest.fixture(scope='function')
def setup_tmp_path():
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        old_cwd = os.getcwd()
        os.chdir(tmpdirname)
        
        dbname =tmpdirname + '/.testdb.sqlite'
        somedir = pathlib.Path(tmpdirname) / 'somedir'
        somedir.mkdir()
        somefile = somedir / 'somefile.txt'
        with somefile.open('w') as f:
            f.write('sometext')

        somedir2 = somedir / 'somedir2'
        somedir2.mkdir()

        somefile1 = somedir2 / 'file1'
        somefile2 = somedir2 / 'file2'
        somefile3 = somedir2 / 'file3'
        somefile4 = somedir2 / 'file4'
        somefile5 = somedir2 / 'file5'
        somefile6 = somedir2 / 'file6'
        
        somefile1.write_text('some content 1')
        somefile2.write_text('some content 2')
        somefile3.write_text('some content 3')
        somefile4.write_text('some content 4')
        somefile5.write_text('some content 5')
        somefile6.write_text('some content 6')

        slnk = somedir2 / 'mysoftlink'
        slnk.symlink_to(somefile1)

        runner = CliRunner()
        runner.invoke(cli, ['--dbname', dbname, 'hash', tmpdirname])

        yield

        os.chdir(old_cwd)

@pytest.mark.usefixtures("setup_tmp_path")
class noTestErrorRaises:
    def test_SymLink_raises(self):
        hashlu = dupetable.get_dupes(dbname = os.getcwd() +'/.testdb.sqlite')
        deldir = os.getcwd()
        deltable, keeptable = dupetable.dd3(hashlu, deldir, pattern=".", dupes_global=True)
        with pytest.raises(DupeIsSymLink):
            mover.validator(keeptable)


    def test_FileNotFound_raises(self):
        mysoftlink = pathlib.Path(os.getcwd() + '/somedir/somedir2/file1')
        mysoftlink.unlink()

        hashlu = dupetable.get_dupes(dbname = os.getcwd() +'/.testdb.sqlite')
        deldir = os.getcwd() + '/somedir'
        deltable, keeptable = dupetable.dd3(hashlu, deldir, pattern=".", dupes_global=True)
        with pytest.raises(DupeFileNotFound):
            mover.validator(keeptable)
