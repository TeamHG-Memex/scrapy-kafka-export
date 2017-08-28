scrapy-kafka-export
===================

.. image:: https://img.shields.io/pypi/v/scrapy-kafka-export.svg
   :target: https://pypi.python.org/pypi/scrapy-kafka-export
   :alt: PyPI Version

.. image:: https://travis-ci.org/TeamHG-Memex/scrapy-kafka-export.svg?branch=master
   :target: http://travis-ci.org/TeamHG-Memex/scrapy-kafka-export
   :alt: Build Status

.. image:: http://codecov.io/github/TeamHG-Memex/scrapy-kafka-export/coverage.svg?branch=master
   :target: http://codecov.io/github/TeamHG-Memex/scrapy-kafka-export?branch=master
   :alt: Code Coverage

``scrapy-kafka-export`` package provides a Scrapy extension to export items
to Kafka.

License is MIT.

Extension requires Python 2.7 or 3.4+.

Install
-------

::

    pip install scrapy-kafka-export

Usage
-----

To use KafkaItemExporterExtension, enable and configure it in settings.py::

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

After that all scraped items would be put to a Kafka topic.
If an item has an ``_id`` field, ``_id`` is used as a message key.

SSL-based auth
~~~~~~~~~~~~~~

If your Kafka uses SSL, configure SSL-based auth::

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

If you're using setup.py to deploy the project (using scrapyd or Scrapy Cloud),
certificates should be added to package data. Modify ``setup.py`` like this::

    from setuptools import setup, find_packages

    setup(
        name = 'myproject',
        ...
        package_data = {
            'myproject': ['certificates/*.pem'],
        },
        ...
    )

Settings
~~~~~~~~

* ``KAFKA_EXPORT_ENABLED`` - Flag that enables the extension;
  it is False by default.
* ``KAFKA_BROKERS`` - List of Kafka brokers in format host:port
* ``KAFKA_TOPIC`` - Kafka topic where items are going to be sent
* ``KAFKA_BATCH_SIZE`` - Kafka batch size (100 by default).
* ``KAFKA_SSL_CONFIG_MODULE`` - name of the project module
* ``KAFKA_SSL_CACERT_FILE`` - resource path of the Certificate Authority
  certificate
* ``KAFKA_SSL_CLIENTCERT_FILE`` - resource path of the client certificate
* ``KAFKA_SSL_CLIENTKEY_FILE`` - resource path of the client key

If ``KAFKA_SSL_CONFIG_MODULE`` is not set, no certificate will be loaded.

Writer
------

If you want to push Scrapy items to Kafka from a script, instead of using
``scrapy_kafka_export.KafkaItemExporterExtension`` use
``scrapy_kafka_export.writer.ScrapyKafkaTopicWriter``; see its docstring
for more.
