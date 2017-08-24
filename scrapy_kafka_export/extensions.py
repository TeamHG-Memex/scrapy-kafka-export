# -*- coding: utf-8 -*-
"""
An extension to export items to a Kafka topic.

Settings
--------
* ``KAFKA_EXPORT_ENABLED`` - Flag that enables the extension;
   it is False by default.
* ``KAFKA_BROKERS`` - List of Kafka brokers in format host:port
* ``KAFKA_TOPIC`` - Kafka topic where items are going to be sent
* ``KAFKA_BATCH_SIZE`` - Kafka batch size (100 by default).

If SSL connection is enabled, the certificates must be included as a python 
module and as package data in setup.py

* ``KAFKA_SSL_CONFIG_MODULE`` - name of the project module
* ``KAFKA_SSL_CACERT_FILE`` - resource path of the Certificate Authority
    certificate
* ``KAFKA_SSL_CLIENTCERT_FILE`` - resource path of the client certificate
* ``KAFKA_SSL_CLIENTKEY_FILE`` - resource path of the client key

If ``KAFKA_SSL_CONFIG_MODULE`` is not set, no certificate will be loaded.

Usage
-----
In ``settings.py``
::

    EXTENSIONS = {
        'scrapy_kafka_export.KafkaItemExporterExtension': 1,
    }
    
    KAFKA_EXPORT_ENABLED = True
    KAFKA_BROKERS = [
        'kafka1:9093',
        'kafka2:9093',
        'kafka3:9093'
    ]
    KAFKA_TOPIC = 'test-topic'
        
    KAFKA_SSL_CONFIG_MODULE = 'myproject'
    KAFKA_SSL_CACERT_FILE = 'certificates/ca-cert.pem'
    KAFKA_SSL_CLIENTCERT_FILE = 'certificates/client-cert.pem'
    KAFKA_SSL_CLIENTKEY_FILE = 'certificates/client-key.pem'

Assuming the following structure for the certificates from the 
project 'myproject'::

    myproject_repo/
    myproject_repo/myproject/
    myproject_repo/myproject/__init_.py
    myproject_repo/myproject/certificates/ca-cert.pem
    myproject_repo/myproject/certificates/myproject-client-cert.pem
    myproject_repo/myproject/certificates/myproject-client-key.pem
    ...

the following package data should be added to ``setup.py``::

    from setuptools import setup, find_packages

    setup(
        name = 'myproject',
        ...
        package_data = {
            'myproject': ['certificates/*.pem'],
        },
        ...
    )
    
"""
import logging
from pkg_resources import resource_filename

from scrapy import signals
from scrapy.exceptions import NotConfigured

from .writer import ScrapyKafkaTopicWriter
from .utils import get_ssl_config

logger = logging.getLogger(__name__)


class KafkaItemExporterExtension(object):
    """ Kafka extension for writing items to a kafka topic """
    def __init__(self, crawler):
        settings = crawler.settings
        if not settings.getbool('KAFKA_EXPORT_ENABLED', False):
            raise NotConfigured
        logger.debug('Kafka export extension is enabled')

        self.kafka_brokers = settings.getlist('KAFKA_BROKERS')
        self.kafka_topic = settings.get('KAFKA_TOPIC')
        self.batch_size = settings.getint('KAFKA_BATCH_SIZE', 100)
        ssl_module_name = settings.get('KAFKA_SSL_CONFIG_MODULE')
        if ssl_module_name:
            def _load(key):
                return resource_filename(ssl_module_name, settings.get(key))
            self.ssl_config = get_ssl_config(
                cafile=_load('KAFKA_SSL_CACERT_FILE'),
                certfile=_load('KAFKA_SSL_CLIENTCERT_FILE'),
                keyfile=_load('KAFKA_SSL_CLIENTKEY_FILE'),
            )
        else:
            self.ssl_config = {}

        self.item_writer = None
        crawler.signals.connect(self.spider_opened, signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signals.spider_closed)
        crawler.signals.connect(self.process_item_scraped,
                                signals.item_scraped)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def spider_opened(self, spider):
        self.item_writer = ScrapyKafkaTopicWriter(
            bootstrap_servers=self.kafka_brokers,
            topic=self.kafka_topic,
            batch_size=self.batch_size,
            **self.ssl_config
        )
        logger.info("Kafka item writer initialized, writing to topic %s." %
                    self.item_writer.topic)
        self._configure_kafka_logging()

    def spider_closed(self, spider):
        if self.item_writer is not None:
            self.item_writer.close()

    def process_item_scraped(self, item, response, spider):
        self.item_writer.write_item(item)

    def _configure_kafka_logging(self):
        """ Disable logging of sent items """
        kafka_logger = logging.getLogger('kafka.producer.kafka')
        kafka_logger.setLevel(logging.INFO)
