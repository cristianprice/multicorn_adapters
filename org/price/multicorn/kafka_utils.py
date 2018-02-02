from logging import NullHandler
from multicorn.utils import log_to_postgres
from multicorn.utils import INFO
from kafka import KafkaConsumer
from kafka import KafkaProducer
import socket


class PgHandler(NullHandler):

    def emit(self, record):
        log_to_postgres("Kafka: {}".format(record), INFO)


def create_producer():
    return KafkaProducer(bootstrap_servers="localhost:9092")


def create_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id=socket.gethostname(),
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        enable_auto_commit=False)
    consumer.subscribe(['Test'])
    return consumer


producer = create_producer()
for i in range(100):
    producer.send(topic='Test', value='Teeeeee {}'.format(i).encode(), key='TTTT'.encode())
    producer.flush()

consumer = create_consumer()
for msg in consumer:
    print(msg)

consumer.close(True)
