from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import INFO
from org.price.multicorn.fs_utils import get_default_root

from org.price.multicorn.fs_utils import walk
from org.price.multicorn.fs_utils import try_get_root
from org.price.multicorn.fs_utils import raise_
from os.path import join
from os.path import getsize


class Fs(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(Fs, self).__init__(options, columns)
        self.debug = (bool)(options.get('debug')) or False
        if self.debug:
            log_to_postgres('Init {}{}'.format(options, columns), INFO)

    def get_rel_size(self, quals, columns):
        if self.debug:
            log_to_postgres('Init {}{}'.format(quals, columns), INFO)
        return 1, 1000

    def execute(self, quals, columns, sortkeys=None):
        root = get_default_root()

        for qual in quals:
            if self.debug:
                log_to_postgres(qual, INFO)
            root = try_get_root(qual) or root

        for location, dirs, files, deeplevel in list(walk(root, onerror=raise_)):
            for file in files:
                yield {"root": root, "depth": deeplevel, "filename": file, "filesize": getsize(join(location, file)), "location": location, "type": 'f'}

            for dir in dirs:
                yield {"root": root, "depth": deeplevel, "filename": dir, "filesize": -1, "location": location, "type": 'd'}
