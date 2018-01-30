from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import ERROR
from multicorn.utils import INFO

from org.price.multicorn.fs_utils import walk
from org.price.multicorn.fs_utils import get_root
from org.price.multicorn.fs_utils import try_get_root
from org.price.multicorn.fs_utils import raise_
from os.path import join
from os.path import getsize
import os
import sys


class Fs(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(Fs, self).__init__(options, columns)
        self.debug = (bool)(options.get('debug')) or False
        if self.debug:
            log_to_postgres('Python Version: {}'.format(sys.version_info), INFO)
            log_to_postgres('Init {}{}'.format(options, columns), INFO)

    def execute(self, quals, columns, sortkeys=None):
        for n in range(10):
            yield {'filename': str(n)}


def nothing():
    for root, dirs, files, deeplevel in list(walk('/li', onerror=raise_)):
        try:
            for file in files:
                yield {"depth": deeplevel, "filename": file, "filesize": getsize(join(root, file)), "location": root, "type": 'f'}

            for dir in dirs:
                yield {"depth": deeplevel, "filename": dir, "filesize": getsize(join(root, file)), "location": root, "type": 'd'}

        except Exception:
            log_to_postgres(str(Exception), ERROR)
