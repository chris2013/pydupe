import os
import pathlib
import tempfile
import shutil

import pytest
from click.testing import CliRunner
from pydupe.cli import cli
import typing as tp


@pytest.fixture(scope='function')
def setup_tmp_path() -> tp.Iterator[pathlib.Path]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        old_cwd = os.getcwd()
        os.chdir(tmpdirname)

        dbname = tmpdirname + '/.testdb.sqlite'

        somedir = pathlib.Path(tmpdirname) / 'somedir'
        somedir.mkdir()

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

        somefile1_cpy = somedir / 'file1_cpy'
        somefile2_cpy = somedir / 'file2_cpy'
        somefile3_cpy = somedir / 'file3_cpy'
        somefile4_cpy = somedir / 'file4_cpy'
        somefile5_cpy = somedir / 'file5_cpy'
        somefile6_cpy = somedir / 'file6_cpy'

        source_lst = [somefile1, somefile2, somefile3,
                      somefile4, somefile5, somefile6]
        target_lst = [somefile1_cpy, somefile2_cpy, somefile3_cpy,
                      somefile4_cpy, somefile5_cpy, somefile6_cpy]
        for source, target in zip(source_lst, target_lst):
            shutil.copy2(src = source, dst= target, follow_symlinks=False )

        runner = CliRunner()
        runner.invoke(cli, ['--dbname', dbname, 'hash', tmpdirname])

        yield pathlib.Path(tmpdirname)

        os.chdir(old_cwd)


# @pytest.mark.usefixtures("setup_tmp_path")
class TestCLI:

    def test_dd(self, setup_tmp_path: pathlib.Path) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        runner = CliRunner()
        runner.invoke(cli, ['--dbname', str(tmpdirname / '.testdb.sqlite'),
                      'dd', '-tr', str(trash), '--do_move', 'somedir/somedir2'])

        result = set()
        for child in trash.rglob('*'):
            if child.is_file():
                result.add(child.name)

        assert result == {'file1', 'file2',
                          'file3', 'file4', 'file5', 'file6', }

    def test_do_move_with_rename_file_1(self, setup_tmp_path: pathlib.Path) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        trash.mkdir()

        fileexist = pathlib.Path(
            str(trash) + str(tmpdirname) + '/somedir/somedir2')
        fileexist.mkdir(parents=True)
        fileexist = fileexist.joinpath('file1')
        fileexist.write_text('some content 1')

        runner = CliRunner()
        runner.invoke(cli, ['--dbname', str(tmpdirname / '.testdb.sqlite'),
                      'dd', '-tr', str(trash), '--do_move', 'somedir/somedir2'])

        result = set()
        for child in trash.rglob('*'):
            if child.is_file():
                result.add(child.name)

        assert result == {'file1', 'file1_1', 'file2',
                          'file3', 'file4', 'file5', 'file6', }

    def test_do_move_with_rename_file_2(self, setup_tmp_path: pathlib.Path) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        trash.mkdir()

        path = pathlib.Path(str(trash) + str(tmpdirname) + '/somedir/somedir2')
        path.mkdir(parents=True)
        fileexist1 = path.joinpath('file1')
        fileexist1.write_text('some content 1')

        fileexist2 = path.joinpath('file1_1')
        fileexist2.write_text('some content 1')

        runner = CliRunner()
        runner.invoke(cli, ['--dbname', str(tmpdirname / '.testdb.sqlite'),
                      'dd', '-tr', str(trash), '--do_move', 'somedir/somedir2'])

        result = set()
        for child in trash.rglob('*'):
            if child.is_file():
                result.add(child.name)

        assert result == {'file1', 'file1_1', 'file1_2', 'file2',
                          'file3', 'file4', 'file5', 'file6', }

    def test_no_symlink_in_database(self, setup_tmp_path: pathlib.Path) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        trash.mkdir()

        slnk = tmpdirname / 'somedir/somedir2/mysoftlink'
        somefile1 = tmpdirname / 'somedir/somedir2/file1'

        slnk.symlink_to(somefile1)

        runner = CliRunner()

        runner.invoke(cli, ['--dbname', str(tmpdirname / '.testdb.sqlite'),
                            'dd', '-tr', str(trash), '--do_move', 'somedir/somedir2'])

        result = set()
        for child in trash.rglob('*'):
            if child.is_file():
                result.add(child.name)

        assert result == {'file1', 'file2',
                          'file3', 'file4', 'file5', 'file6', }

    def test_delete_file(self, setup_tmp_path: pathlib.Path) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        trash.mkdir()

        path = pathlib.Path(str(trash) + str(tmpdirname) + '/somedir/somedir2')
        path.mkdir(parents=True)
        fileexist1 = path.joinpath('file1')
        fileexist1.write_text('some content 1')

        fileexist2 = path.joinpath('file1_1')
        fileexist2.write_text('some content 1')

        runner = CliRunner()
        runner.invoke(cli, ['--dbname', str(tmpdirname / '.testdb.sqlite'),
                     'dd', '-tr', 'DELETE', '--do_move', 'somedir/somedir2'])

        result = set()
        fldr = tmpdirname / 'somedir/somedir2'
        for child in fldr.rglob('*'):
            if child.is_file():
                result.add(child.name)

        assert result == set()

        result = set()
        fldr = tmpdirname
        for child in fldr.rglob('*'):
            if child.is_dir():
                result.add(child.name)

        assert 'DELETE' not in result