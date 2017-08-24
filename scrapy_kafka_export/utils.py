# -*- coding: utf-8 -*-
import logging
from traceback import format_exc

DONT_RETRY_ERRORS = (KeyboardInterrupt, SystemExit, ImportError)


def just_log_exception(exception):
    logging.error(format_exc())
    return not isinstance(exception, DONT_RETRY_ERRORS)
