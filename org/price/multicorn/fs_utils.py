from os.path import join, isdir, islink
from os import error, listdir


def walk(top, topdown=True, onerror=None, deeplevel=0):
    try:
        names = listdir(top)
    except error:
        if onerror is not None:
            onerror(error)
        return

    dirs, nondirs = [], []
    for name in names:
        if isdir(join(top, name)):
            dirs.append(name)
        else:
            nondirs.append(name)

    if topdown:
        yield top, dirs, nondirs, deeplevel
    for name in dirs:
        path = join(top, name)
        if not islink(path):
            for x in walk(path, topdown, onerror, deeplevel + 1):
                yield x
    if not topdown:
        yield top, dirs, nondirs, deeplevel
