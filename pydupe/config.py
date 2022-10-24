import platform
import logging
import typing as tp
import pathlib

SYSTEM_ = platform.system()

if SYSTEM_ == 'Linux':
    HASHEXECUTE_1 = ['sha256sum']
elif SYSTEM_ == 'FreeBSD':
    HASHEXECUTE_1 = ['shasum', '-a', '256']
else:
    raise SystemExit("pydupe not yet verified on " + SYSTEM_)
cnf : tp.Dict[str, tp.Any]= {}
cnf['HASHEXECUTE_1'] = HASHEXECUTE_1

cnf['SYSTEM'] = SYSTEM_

CONFIGFLAGFILE = pathlib.Path.home() / ".pydupe.log"
if CONFIGFLAGFILE.exists():
    cnf['LOGLEVEL'] = logging.DEBUG
else:
    cnf['LOGLEVEL'] = logging.ERROR