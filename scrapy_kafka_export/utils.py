# -*- coding: utf-8 -*-
import logging
from traceback import format_exc

DONT_RETRY_ERRORS = (KeyboardInterrupt, SystemExit, ImportError)


def just_log_exception(exception):
    logging.error(format_exc())
    return not isinstance(exception, DONT_RETRY_ERRORS)


def get_ssl_config(cafile, certfile, keyfile):
    return {
        'security_protocol': 'SSL',
        'ssl_cafile': cafile,
        'ssl_certfile': certfile,
        'ssl_keyfile': keyfile,
        'ssl_check_hostname': False
    }
