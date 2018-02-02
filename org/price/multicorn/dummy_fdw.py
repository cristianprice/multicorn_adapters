from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import INFO


class DummyFdw(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(DummyFdw, self).__init__(options, columns)
        log_to_postgres('__init__  {} {}'.format(options, columns), INFO)

    def execute(self, quals, columns, sortkeys=None):
        log_to_postgres('execute  {} {} {}'.format(quals, columns, sortkeys), INFO)
        pass

    @property
    def rowid_column(self):
        log_to_postgres('rowid_column: id', INFO)
        return 'id'

    def insert(self, values):
        log_to_postgres('insert: {}'.format(values), INFO)
        return values

    def update(self, oldvalues, newvalues):
        log_to_postgres('update: {} {}'.format(oldvalues, newvalues), INFO)
        return oldvalues

    def commit(self):
        log_to_postgres('commit:', INFO)
        pass

    def rollback(self):
        log_to_postgres('rollback:', INFO)
        pass
