from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import INFO
from kafka import KafkaConsumer
from kafka import KafkaProducer
import socket


class KafkaFdw(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(KafkaFdw, self).__init__(options, columns)
        self.debug = options.get('debug') == 'True'
        self.consumer_topic = options.get('consumer_topic')
        self.producer_topic = options.get('producer_topic')
        self.group_id = options.get('group_id') or socket.gethostname()
        self.auto_commit = options.get('auto_commit') == 'True'
        self.bootstrap_servers = options.get('bootstrap_servers') or 'localhost'

        if not self.producer_topic or not self.consumer_topic:
            raise IOError('Please supply a consumer and producer topic name.')

        if self.debug:
            log_to_postgres('Init {}{}'.format(options, columns), INFO)

        if self.debug:
            log_to_postgres('Creating consumer ...', INFO)
        self.consumer = self.create_consumer()

        if self.debug:
            log_to_postgres('Creating producer ...', INFO)
        self.producer = self.create_producer()

    def create_consumer(self):
        return KafkaConsumer(self.consumer_topic, bootstrap_servers=self.bootstrap_servers, group_id=self.group_id,
                             enable_auto_commit=self.auto_commit)

    def create_producer(self):
        return KafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def execute(self, quals, columns, sortkeys=None):
        pass

    def rowid_column(self):
        return "__rowid__"

    def insert(self, values):
        log_to_postgres(values, INFO)
        return {}

    def update(self, oldvalues, newvalues):
        log_to_postgres(oldvalues, newvalues, INFO)
        return {}

    def commit(self):
        pass

    def rollback(self):
        pass
