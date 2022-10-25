import os
from pathlib import Path as p
import tempfile
import shutil

import pytest
from click.testing import CliRunner
from pydupe.cli import cli
import typing as tp

from pydupe.db import PydupeDB


runner = CliRunner()

@pytest.fixture()
def setup_tmp_path() -> tp.Iterator[p]:
    """ Fixture to set up PydupeDB in tmporary Directory"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        old_cwd = os.getcwd()
        os.chdir(tmpdirname)

        dbname = tmpdirname + '/.testdb.sqlite'

        somedir = p(tmpdirname) / 'somedir'
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
            shutil.copy2(src=source, dst=target, follow_symlinks=False)

        runner = CliRunner()
        runner.invoke(cli, ['--dbname', dbname, 'hash', tmpdirname])

        yield p(tmpdirname)

        os.chdir(old_cwd)


class TestCLI:

    def test_dd(self, setup_tmp_path: p) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        runner = CliRunner()
        runner.invoke(cli, ['--dbname', str(tmpdirname) + '/.testdb.sqlite',
                      'dd', '-tr', str(trash), '--do_move', str(tmpdirname) + '/somedir/somedir2'])

        result = set()
        for child in trash.rglob('*'):
            if child.is_file():
                result.add(child.name)

        assert result == {'file1', 'file2',
                          'file3', 'file4', 'file5', 'file6', }

    def test_do_move_with_rename_file_1(self, setup_tmp_path: p) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        trash.mkdir()

        fileexist = p(
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

    def test_do_move_with_rename_file_2(self, setup_tmp_path: p) -> None:
        tmpdirname = setup_tmp_path
        trash = tmpdirname / '.pydupeTrash'
        trash.mkdir()

        path = p(str(trash) + str(tmpdirname) + '/somedir/somedir2')
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

    def test_no_symlink_in_database(self, setup_tmp_path: p) -> None:
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

    def test_purge(self, setup_tmp_path: p) -> None:
        tmpdirname = setup_tmp_path
        dbname = tmpdirname / '.testdb.sqlite'
        added_file = tmpdirname / 'addedfile'
        added_file.write_text('some text')
        added_file2 = tmpdirname / 'addedfile2'
        added_file2.write_text('some text')

        runner.invoke(cli, ['--dbname', str(dbname), 'hash', '.'])

        added_file.unlink()

        with PydupeDB(dbname) as db:
            db.copy_lookup_to_permanent()
            filelist_in_permanent = [x['filename']
                                     for x in db.get_files_in_permanent()]
        assert str(added_file) in filelist_in_permanent
        assert str(added_file2) in filelist_in_permanent

        runner.invoke(cli, ['--dbname', str(dbname), 'purge'])

        with PydupeDB(dbname) as db:
            filelist_in_permanent = [x['filename']
                                     for x in db.get_files_in_permanent()]

        assert str(added_file) not in filelist_in_permanent
        assert str(added_file2) in filelist_in_permanent

    def test_clean(self, setup_tmp_path: p) -> None:
        tmpdirname = setup_tmp_path
        dbname = tmpdirname / '.testdb.sqlite'

        with PydupeDB(dbname) as db:
            filelist_in_lookup = sorted(
                [str(p(x['filename']).relative_to(tmpdirname)) for x in db.get()])
        assert filelist_in_lookup == ['somedir/file1_cpy', 'somedir/file2_cpy', 'somedir/file3_cpy', 'somedir/file4_cpy', 'somedir/file5_cpy', 'somedir/file6_cpy',
                                      'somedir/somedir2/file1', 'somedir/somedir2/file2', 'somedir/somedir2/file3', 'somedir/somedir2/file4', 'somedir/somedir2/file5', 'somedir/somedir2/file6']

        runner.invoke(cli, ['--dbname', str(dbname), 'clean'])

        with PydupeDB(dbname) as db:
            filelist_in_lookup = sorted(
                [str(p(x['filename']).relative_to(tmpdirname)) for x in db.get()])
        assert filelist_in_lookup == []