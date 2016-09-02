#!/usr/bin/env python
# coding=utf8

import sys
import logging
from logging.handlers import TimedRotatingFileHandler

logger_cache = dict()


def get_logger_by_name(log_name="", *args, **kwargs):
    if log_name in logger_cache:
        return logger_cache[log_name]
    else:
        log = logger(log_name, *args, **kwargs)
        logger_cache[log_name] = log
        return log


def logger(log_name="", stream=sys.stdout, stream_log_level=logging.INFO, \
           log_abspath=None, file_log_level=logging.WARNING):
    """
    A good convention to use when naming loggers is to use a module-level logger
    logger = logging.getLogger(__name__)
    """
    LOG = logging.getLogger(log_name)
    log_level = min(stream_log_level, file_log_level)
    LOG.setLevel(log_level)
    if log_level == logging.DEBUG:
        fmt_str = '%(levelname)s %(asctime)s [%(module)s:%(lineno)d] (%(process)d:%(thread)d)\t %(message)s'
    else:
        fmt_str = '%(levelname)s %(asctime)s [%(name)s]:\t %(message)s'
    formatter = logging.Formatter(fmt=fmt_str, datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler = logging.StreamHandler(stream=stream)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(stream_log_level)
    LOG.addHandler(stream_handler)

    if log_abspath:
        file_handler = TimedRotatingFileHandler(log_abspath, when='midnight', backupCount=30)
        file_handler.setLevel(file_log_level)
        file_handler.setFormatter(formatter)
        LOG.addHandler(file_handler)

    return LOG