import platform
import logging
import typing as tp
import pathlib

SYSTEM_ = platform.system()

if SYSTEM_ == 'Linux':
    HASHEXECUTE_1 = ['sha256sum']
    HASHEXECUTE_2: list[tp.Optional[str]] = []
elif SYSTEM_ == 'FreeBSD':
    HASHEXECUTE_1 = ['shasum', '-a', '256']
    HASHEXECUTE_2 = []
# elif SYSTEM_ == 'Windows':
#     HASHEXECUTE_1 = ['certUtil', '-hashfile']
#     HASHEXECUTE_2 = ['SHA256']
else:
    raise SystemExit("pydupe not yet verified on " + SYSTEM_)
cnf : tp.Dict[str, tp.Any]= {}
cnf['HASHEXECUTE_1'] = HASHEXECUTE_1
cnf['HASHEXECUTE_2'] = HASHEXECUTE_2

cnf['SYSTEM'] = SYSTEM_

CONFIGFLAGFILE = pathlib.Path.home() / ".pydupe.log"
if CONFIGFLAGFILE.exists():
    cnf['LOGLEVEL'] = logging.DEBUG
else:
    cnf['LOGLEVEL'] = logging.ERROR