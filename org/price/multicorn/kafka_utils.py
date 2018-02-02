from logging import NullHandler
from multicorn.utils import log_to_postgres
from multicorn.utils import INFO


class PgHandler(NullHandler):

    def emit(self, record):
        log_to_postgres("Kafka: {}".format(record), INFO)
