from multicorn import ForeignDataWrapper
import logging

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)


class Fs(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(Fs, self).__init__(options, columns)
        self.logger = logging.getLogger('Fs')

        self.logger.warning('Init: %s %s', options, columns)

    def get_rel_size(self, quals, columns):
        self.logger.warning('Init: %s %s', quals, columns)
        return 1, 1000

    def get_path_keys(self):
        return [(('FileName',), 1), (('Depth',), 1), (('Location',), 1), (('FileSize',), 100), (('Type',), 10)]

    def execute(self, quals, columns, sortkeys=None):
        self.logger.warning('Init: %s %s %s', quals, columns, sortkeys)
        pass
