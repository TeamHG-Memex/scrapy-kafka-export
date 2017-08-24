# -*- coding: utf-8 -*-
from kafka import KafkaProducer, KafkaConsumer
from retrying import retry
from scrapy.exporters import PythonItemExporter
from scrapy.utils.python import to_bytes
from scrapy.utils.serialize import ScrapyJSONEncoder

from .utils import just_log_exception, get_ssl_config


class UnknownTopicError(Exception):
    pass


class KafkaTopicWriter(object):
    """ Kafka Writer which puts objects to a Kafka topic.
    It retries sending in case of errors, checks that a topic exists,
    and changes some of the defaults.
    """

    # seconds to wait before closing the producer
    KAFKA_PRODUCER_CLOSE_TIMEOUT = 180

    def __init__(self, bootstrap_servers, topic, batch_size,
                 ssl_cafile=None, ssl_certfile=None, ssl_keyfile=None,
                 compression_type='gzip', validate_topic=True,
                 **kwargs):
        _kwargs = {
            'retry_backoff_ms': 30000,  # unlimited retries by default
            'batch_size': batch_size,
            'max_request_size': 10 * 1024 * 1024,
            'request_timeout_ms': 120000,
            'compression_type': compression_type
        }

        if ssl_cafile is not None:
            self.ssl_config = get_ssl_config(ssl_cafile, ssl_certfile,
                                             ssl_keyfile)
            _kwargs.update(self.ssl_config)
        else:
            self.ssl_config = {}

        _kwargs.update(kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        if validate_topic:
            self._validate_topic()
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      **_kwargs)

    def write(self, key, msg):
        key = None if key is None else to_bytes(key)
        return self._send_message(key, msg, self.topic)

    def close(self):
        self.producer.flush(timeout=self.KAFKA_PRODUCER_CLOSE_TIMEOUT)
        self.producer.close(timeout=self.KAFKA_PRODUCER_CLOSE_TIMEOUT)

    def _validate_topic(self):
        if self.topic not in self._get_topic_list():
            raise UnknownTopicError("Topic %s does not exist" % self.topic)

    @retry(wait_fixed=60000, retry_on_exception=just_log_exception)
    def _send_message(self, key, msg, topic):
        """ Send the message to Kafka using KeyedProducer interface. """
        self.producer.send(topic=topic, value=msg, key=key)
        return True

    def _get_topic_list(self):
        """ Return a list of all Kafka topics """
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                 **self.ssl_config)

        @retry(wait_fixed=60000, retry_on_exception=just_log_exception)
        def get_topics():
            return consumer.topics()

        try:
            return get_topics()
        finally:
            consumer.close()


class ScrapyKafkaTopicWriter(KafkaTopicWriter):
    """ Kafka writer which knows how to handle Scrapy items: they are
    serialized to JSON, and "_id" field is used as Kafka key if present.
    """
    def __init__(self, *args, **kwargs):
        self._encoder = ScrapyJSONEncoder()
        self._exporter = PythonItemExporter(binary=False)
        kwargs.setdefault('value_serializer', self._serialize_value)
        super(ScrapyKafkaTopicWriter, self).__init__(*args, **kwargs)

    def write_item(self, item):
        key = item.get('_id', None)
        msg = self._exporter.export_item(item)
        return self.write(key, msg)

    def _serialize_value(self, value):
        return self._encoder.encode(value).encode('utf8')
