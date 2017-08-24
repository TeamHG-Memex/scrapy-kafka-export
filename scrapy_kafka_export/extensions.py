# -*- coding: utf-8 -*-
"""
Exports items to a Kafka topic

Settings
--------
* ``KAFKA_EXPORT_ENABLED`` - Flag that enables the extension
* ``KAFKA_BROKERS`` - List of Kafka brokers in format host:port
* ``KAFKA_TOPIC`` - Kafka topic where items are going to be sent

If SSL connection is enabled, the certificates must be included as a python 
module and as package data in setup.py

* ``SSL_CONFIG_MODULE`` - name of the project module
* ``SSL_CACERT_FILE`` - resource path of the Certificate Authority certificate
* ``SSL_CLIENTCERT_FILE`` - resource path of the client certificate
* ``SSL_CLIENTKEY_FILE`` - resource path of the client key

If ``SSL_CONFIG_MODULE`` is not set, no certificate will be loaded

Spider attributes
-----------------

The following spider attributes are available and overrides equivalent settings:

* ``kafka_export_enabled`` - Same as ``KAFKA_EXPORT_ENABLED``
* ``kafka_topic`` - Same as ``KAFKA_TOPIC``

Usage
-----
In ``settings.py``
::

    EXTENSIONS = {
        'scrapy-kafka-export.extensions.KafkaItemExporterExtension': 1,
    }
    
    KAFKA_EXPORT_ENABLED = True
    KAFKA_BROKERS = [
        'kafka1:9093',
        'kafka2:9093',
        'kafka3:9093'
    ]
    KAFKA_TOPIC = 'test-topic'
        
    SSL_CONFIG_MODULE = 'myproject'
    SSL_CACERT_FILE = 'certificates/ca-cert.pem'
    SSL_CLIENTCERT_FILE = 'certificates/client-cert.pem'
    SSL_CLIENTKEY_FILE = 'certificates/client-key.pem'

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

from scrapy import signals
from scrapy.exceptions import NotConfigured

from .config import KafkaItemExporterConfigs
from .writer import UnknownTopicError, ScrapyKafkaTopicWriter

logger = logging.getLogger(__name__)


class KafkaItemExporterExtension(object):
    """ Kafka extension for writing items to a kafka topic """
    def __init__(self, crawler):
        self.config = KafkaItemExporterConfigs(crawler.settings)
        self.item_writer = None

        crawler.signals.connect(self.spider_opened, signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signals.spider_closed)
        crawler.signals.connect(self.process_item_scraped, signals.item_scraped)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def spider_opened(self, spider):
        self.config.set_spider(spider)
        if not self.config.is_enabled:
            logger.debug('Kafka item exporter not enabled.')
            return

        self.initialize_kafka_producer(spider)

    def initialize_kafka_producer(self, spider):
        try:
            self.item_writer = ScrapyKafkaTopicWriter(
                bootstrap_servers=self.config.kafka_brokers,
                topic=self.config.kafka_topic,
                batch_size=self.config.batch_size,
                **self.config.ssl_config
            )
        except UnknownTopicError as e:
            logger.error(e.args[0])
            raise NotConfigured
        logger.debug("Kafka writer initialized.")

    def spider_closed(self, spider):
        if self.item_writer is not None:
            self.item_writer.close()

    def process_item_scraped(self, item, response, spider):
        if self.config.is_enabled:
            self.item_writer.write_item(item)
