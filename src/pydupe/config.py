import platform

SYSTEM_ = platform.system()

if SYSTEM_ == 'Linux':
    HASHEXECUTE = ['sha256sum']
elif SYSTEM_ == 'FreeBSD':
    HASHEXECUTE = ['shasum', '-a', '256']

cnf = {}
cnf['HASHEXECUTE'] = HASHEXECUTE
cnf['SYSTEM'] = SYSTEM_
