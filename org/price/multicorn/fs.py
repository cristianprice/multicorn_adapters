from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import ERROR
from multicorn.utils import INFO
import string
import platform
from org.price.multicorn.fs_utils import walk
from os.path import join
from os.path import getsize


def raise_(ex):
    raise ex


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
        location = self.get_root()

        columns = sorted(columns)
        if self.debug:
            log_to_postgres("Sorted columns: {}".format(columns), INFO)

        for qual in quals:
            if self.debug:
                log_to_postgres(qual, INFO)
            location = self.try_get_location(qual) or location

        if self.debug:
            log_to_postgres('Selected Location : {}'.format(location), INFO)

        if self.debug:
            log_to_postgres('Execute: {} {} {}'.format(quals, columns, sortkeys), INFO)

        for root, dirs, files, deeplevel in list(walk(location, onerror=raise_)):
            if self.debug:
                log_to_postgres('File {} {} {} '.format(root, dirs, files), INFO)

        try:
            for file in files:
                yield {"depth": deeplevel, "filename": file, "filesize": getsize(join(root, file)), "location": root, "type": 'f'}

            for dir in dirs:
                yield {"depth": deeplevel, "filename": dir, "filesize": getsize(join(root, file)), "location": root, "type": 'd'}

        except Exception:
            log_to_postgres(str(Exception), ERROR)

    def try_get_location(self, qual):
        if qual.field_name == 'location':
            if qual.operator == '=':
                return qual.value

            log_to_postgres('Unsupported operator {}'.format(qual.operator), ERROR)
        return None

    def get_root(self):
        system = platform.system()

        if system.lower() == 'windows':
            return self.get_drives()[0] + '\\'

        return '/'

    def get_drives(self):
        from ctypes import windll
        drives = []
        bitmask = windll.kernel32.GetLogicalDrives()
        for letter in string.uppercase:
            if bitmask & 1:
                drives.append(letter)
            bitmask >>= 1

        return drives
