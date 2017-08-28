# -*- coding: utf-8 -*-

import os
import random
import string
from contextlib import contextmanager

import pytest
from kafka import SimpleClient, SimpleConsumer
from scrapy.utils.serialize import ScrapyJSONDecoder
from scrapy.utils.test import get_crawler
from scrapy_kafka_export import KafkaItemExporterExtension
from scrapy_kafka_export.writer import UnknownTopicError


def random_string(l):
    return "".join(random.choice(string.ascii_letters) for i in range(l))

def consume_from_kafka(client, topic):
    consumer = SimpleConsumer(client=client, group=None, topic=topic)
    consumer.seek(-1, 2)
    msg = consumer.get_message(block=False, timeout=10)
    consumer.stop()
    if msg:
        deserializer = ScrapyJSONDecoder()
        return deserializer.decode(msg.message.value.decode('utf8'))
    raise Exception

def run_command(command):
    import subprocess
    return subprocess.check_output(['bash','-c', command])

def create_topic(topic):
    bash_command = "{}/kafka-topics.sh --create " \
                   "--zookeeper {} --replication-factor 1 " \
                   "--partitions 1 --topic {}".format(
        os.getenv('KAFKA_BIN'), os.getenv('ZOOKEEPER'), topic
    )
    return run_command(bash_command)

def delete_topic(topic):
    bash_command = "{}/kafka-topics.sh --delete " \
                   "--zookeeper {} --topic {}".format(
        os.getenv('KAFKA_BIN'), os.getenv('ZOOKEEPER'), topic
    )
    return run_command(bash_command)

@contextmanager
def opened_middleware(crawler):
    mw = KafkaItemExporterExtension.from_crawler(crawler)
    crawler.spider = crawler._create_spider('example')
    mw.spider_opened(crawler.spider)
    try:
        yield mw
    finally:
        mw.spider_closed(crawler.spider)

class TestClass:
    topic = None
    broker = None

    @classmethod
    def setup_class(cls):
        cls.broker = os.getenv('KAFKA_BROKER')
        if not cls.topic:
            topic = "%s-%s" % ('topic_test_', random_string(10))
            cls.topic = topic

        cls.client = SimpleClient(cls.broker)
        output = create_topic(cls.topic)

    @classmethod
    def teardown_class(cls):
        output = delete_topic(cls.topic)
        cls.client.close()

    def test_crawl(self):
        settings = {
            'KAFKA_BROKERS': [self.broker],
            'KAFKA_TOPIC': self.topic,
            'KAFKA_EXPORT_ENABLED': True
        }
        crawler = get_crawler(settings_dict=settings)
        with opened_middleware(crawler) as mw:

            assert mw.item_writer is not None
            item1 = {'_id': '0001', 'body': 'message 01'}
            mw.process_item_scraped(item1, response=None, spider=crawler.spider)

        kafka_item1 = consume_from_kafka(self.client, self.topic)
        print (kafka_item1, item1)
        assert item1 == kafka_item1

    def test_unkown_topic_exception(self):
        settings = {
            'KAFKA_BROKERS': [self.broker],
            'KAFKA_TOPIC': 'invalid_topic',
            'KAFKA_EXPORT_ENABLED': True
        }
        crawler = get_crawler(settings_dict=settings)
        with pytest.raises(UnknownTopicError) as e_info:
            with opened_middleware(crawler) as mw:
                pass