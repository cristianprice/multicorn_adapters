from os.path import join, isdir, islink
from os import error, listdir
from tempfile import gettempdir
import platform
from multicorn.utils import log_to_postgres
from multicorn.utils import ERROR


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


def get_default_root():
    return gettempdir()


def try_get_root(qual):
    if qual.field_name == 'root':
        if qual.operator == '=':
            return qual.value

        log_to_postgres('Unsupported operator {}'.format(qual.operator), ERROR)
    return None


def raise_(ex):
    raise ex
