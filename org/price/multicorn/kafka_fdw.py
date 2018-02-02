from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import INFO
from kafka import KafkaConsumer
from kafka import KafkaProducer
import socket
import logging

from org.price.multicorn.kafka_utils import PgHandler


class KafkaFdw(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(KafkaFdw, self).__init__(options, columns)
        self.debug = options.get('debug') == 'True'
        self.consumer_topic = options.get('consumer_topic')
        self.producer_topic = options.get('producer_topic')
        self.group_id = options.get('group_id') or socket.gethostname()
        self.auto_commit = options.get('auto_commit') == 'True'
        self.bootstrap_servers = options.get('bootstrap_servers') or 'localhost'

        self.logger = logging.getLogger('kafka')
        self.logger.addHandler(PgHandler())
        self.logger.setLevel(logging.DEBUG)

        self.row_id = 'id'

        if self.debug:
            log_to_postgres('RowId: {}'.format(self.row_id), INFO)

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
        return KafkaConsumer(self.consumer_topic,
                             key_deserializer=lambda x: x.decode('utf-8'),
                             value_deserializer=lambda x: x.decode('utf-8'),
                             bootstrap_servers=self.bootstrap_servers,
                             auto_offset_reset='latest',
                             group_id=self.group_id,
                             consumer_timeout_ms=1000,
                             enable_auto_commit=self.auto_commit)

    def create_producer(self):
        return KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                             key_serializer=lambda x: x.encode('utf-8'),
                             value_serializer=lambda x: x.encode('utf-8'))

    def execute(self, quals, columns, sortkeys=None):
        count = 0
        ret = {}

        if self.debug:
            log_to_postgres('Iterating: {}'.format(self.consumer), INFO)

        for msg in self.consumer:
            ret['msg'] = msg.value
            count += 1
            ret['id'] = str(count)
            if self.debug:
                log_to_postgres('Ret: {}'.format(ret), INFO)
            yield ret

    @property
    def rowid_column(self):
        return self.row_id

    def insert(self, values):
        if self.debug:
            log_to_postgres(values, INFO)

        self.producer.send(topic=self.producer_topic, value=values['msg'])
        self.producer.flush()
        return values

    def commit(self):
        self.consumer.commit();
        pass

    def rollback(self):
        pass
