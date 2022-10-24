from pydupe.cli import cli
import pytest
from click.testing import CliRunner

def notest_dd() -> None:
    runner = CliRunner()
    runner.invoke(cli, [ 'hash', '/home/chris'])

