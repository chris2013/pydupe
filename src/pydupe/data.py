import attrs
from typing import Optional, Dict, List, Any
import pathlib as pl
import sqlite3
import re

valid_sha256 = re.compile(r"^[a-f0-9]{64}(:.+)?$", re.IGNORECASE)

# measurement with 18393 files:
# w/  validation: 194.84 sec
# w/o validation: 186.13 sec 
# difference: 5%

# w/  attrs: 194.84 sec (w/ validation)
# w/o attrs: 175.92 sec
# difference: 10%

# with sqlite3.executemany:
# 194.84 sec (w/o executemany, w/ attrs, w/ validation)
# 187.96 sec (w/  executemany, w/ attrs, w/ validation)
# difference: 3.5 %

# 194.84 sec w/ sqlite3.executemany, w/attrs, w/  validation, chunkĺength 50
# (188.99 sec w/ sqlite3.executemany, w/attrs, w/o validation, chunklength 50)
# 168.64 sec w/ sqlite3.executemany, w/attrs, w/o validation, chunklength 1000
# difference: 13%

# decision: take attrs with hash validation and use sqlite3.executemany

@attrs.define(slots=True, kw_only=True, order=True)
class fparms:
    filename: Optional[str] = None
    hash: Optional[str] = attrs.field(default=None)
    size: Optional[int] = None
    inode: Optional[int] = None
    mtime: Optional[float] = None
    ctime: Optional[float] = None
    @hash.validator
    def check(self, attribute: Any, value: str)-> None:
        if value and not valid_sha256.match(value):
            raise ValueError("not a valid sha256 hash")

    @classmethod
    def from_path(cls, pth: pl.Path, hash: Optional[str] = None) -> 'fparms':
        stat = pth.stat()
        return cls(filename=str(pth), hash=hash, size=stat.st_size, inode=stat.st_ino, mtime=stat.st_mtime, ctime=stat.st_ctime)
    @classmethod
    def from_row(cls, row: sqlite3.Row) -> 'fparms':
        return cls(filename=row['filename'], hash=row['hash'], size=row['size'], inode=row['inode'], mtime=row['mtime'], ctime=row['ctime'])