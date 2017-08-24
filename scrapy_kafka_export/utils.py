# -*- coding: utf-8 -*-
import logging
from traceback import format_exc


def just_log_exception(exception):
    logging.error(format_exc())
    for etype in (KeyboardInterrupt, SystemExit, ImportError):
        if isinstance(exception, etype):
            return False
    return True  # retries any other exception