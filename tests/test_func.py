import pytest
import pathlib
import collections
import pydupe.dupetable as dupetable

cwd = str(pathlib.Path.cwd())
tdata = cwd + "/pydupe/pydupe/tests/tdata/"
home = str(pathlib.Path.home())

def test_sanitize_dict():
    a = {1:2,3:4,5:collections.deque([]),6:collections.deque(["item1","item2"]), 7: None, 8: 8, 9: 9.1}
    b = dupetable.sanitize_dict(a)
    assert b == {1:2,3:4,6:collections.deque(["item1","item2"]), 7: None, 8: 8, 9: 9.1}