scrapy-kafka-export
===================

scrapy-kafka-export package provides a Scrapy extension to export items
to Kafka.

For items which have an ``_id`` field, ``_id`` is used as a message key.
