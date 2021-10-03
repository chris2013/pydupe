import pytest
import pathlib

from pydupe.dupetable import Dupetable

cwd = str(pathlib.Path.cwd())
tdata = cwd + "/pydupe/pydupe/tests/tdata/"
home = str(pathlib.Path.home())

def test_sanitize_dict():
    a = {1:2,3:4,5:[],6:["item1","item2"], 7: None, 8: 8, 9: 9.1}
    b = Dupetable.sanitize_dict(a)
    assert b == {1:2,3:4,6:["item1","item2"], 7: None, 8: 8, 9: 9.1}