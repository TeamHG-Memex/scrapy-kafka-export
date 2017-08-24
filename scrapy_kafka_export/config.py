# -*- coding: utf-8 -*-
from pkg_resources import resource_filename

class KafkaItemExporterConfigs(object):

    def __init__(self, settings):
        self.settings = settings
        self.spider = None

    def set_spider(self, spider):
        self.spider = spider

    @property
    def is_enabled(self):
        return (getattr(self.spider, 'kafka_export_enabled', False) or
                self.settings.getbool('KAFKA_EXPORT_ENABLED', False))

    @property
    def kafka_brokers(self):
        return self.settings.getlist('KAFKA_BROKERS')

    @property
    def kafka_topic(self):
        return (getattr(self.spider, 'kafka_topic', None) or
                self.settings.get('KAFKA_TOPIC'))

    @property
    def batch_size(self):
        return self.settings.getint('PRODUCER_BATCH_SIZE', 1000)

    @property
    def ssl_config(self):
        module_name = self.settings.get('SSL_CONFIG_MODULE', None)
        if not module_name:
            return {}

        def get_from_module(module_name, key):
            return resource_filename(module_name, self.settings.get(key))

        return {
            'security_protocol': 'SSL',
            'ssl_cafile': get_from_module(module_name, 'SSL_CACERT_FILE'),
            'ssl_certfile': get_from_module(module_name, 'SSL_CLIENTCERT_FILE'),
            'ssl_keyfile': get_from_module(module_name, 'SSL_CLIENTKEY_FILE'),
            'ssl_check_hostname': False
        }