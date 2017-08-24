# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from retrying import retry
from scrapy.utils.python import to_bytes
from scrapy.utils.serialize import ScrapyJSONEncoder

from .utils import just_log_exception

_encoder = ScrapyJSONEncoder()


def serialize_value(value):
    return _encoder.encode(value).encode('utf8')


class KafkaKeyedWriter(object):
    """ Kafka Writer, using gzipped JSON.
    It retries sending in case of errors.
    """

    # seconds to wait before closing the producer
    KAFKA_PRODUCER_CLOSE_TIMEOUT = 180

    def __init__(self, bootstrap_servers, topic, batch_size,
                 compression_type='gzip', value_serializer=serialize_value,
                 **kwargs):
        _kwargs = {
            # unlimited retries by default
            'retry_backoff_ms': 30000,
            'batch_size': batch_size,
            'max_request_size': 10 * 1024 * 1024,
            'request_timeout_ms': 120000,
            'compression_type': compression_type
        }
        _kwargs.update(kwargs)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=value_serializer,
                                      **_kwargs)
        self.topic = topic

    def write(self, key, msg):
        key = None if key is None else to_bytes(key)
        return self._send_message(key, msg, self.topic)

    def close(self):
        self.producer.flush(timeout=self.KAFKA_PRODUCER_CLOSE_TIMEOUT)
        self.producer.close(timeout=self.KAFKA_PRODUCER_CLOSE_TIMEOUT)

    @retry(wait_fixed=60000, retry_on_exception=just_log_exception)
    def _send_message(self, key, msg, topic):
        """ Send the message to Kafka using KeyedProducer interface. """
        self.producer.send(topic=topic, value=msg, key=key)
        return True
