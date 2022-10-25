from typing import Optional, NamedTuple
from pathlib import Path as p
import sqlite3
import re

valid_sha256 = re.compile(r"^[a-f0-9]{64}(:.+)?$", re.IGNORECASE)

class fparms(NamedTuple):
    filename: Optional[str] = None
    hash: Optional[str] = None
    size: Optional[int] = None
    inode: Optional[int] = None
    mtime: Optional[float] = None
    ctime: Optional[float] = None

def checkHash(fp: fparms) -> None:
    if fp.hash and not valid_sha256.match(fp.hash):
        raise ValueError("not a valid sha256 hash")

def from_path(pth: p, hash: Optional[str] = None) -> fparms:
    stat = pth.stat()
    return fparms(filename=str(pth.resolve()), hash=hash, size=stat.st_size, inode=stat.st_ino, mtime=stat.st_mtime, ctime=stat.st_ctime)

def from_row(row: sqlite3.Row) -> fparms:
    return fparms(filename=row['filename'], hash=row['hash'], size=row['size'], inode=row['inode'], mtime=row['mtime'], ctime=row['ctime'])