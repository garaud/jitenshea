# coding: utf-8

"""Jitenshea package

Bicycle-sharing analysis
"""

import os
import logging
import configparser

import daiquiri
import daiquiri.formatter


FORMAT = '%(asctime)s :: %(color)s%(levelname)s :: %(name)s :: %(funcName)s : %(message)s%(color_stop)s'
daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(formatter=daiquiri.formatter.ColorFormatter(
        fmt=FORMAT)),
    ))
logger = daiquiri.getLogger("root")


_ROOT = os.path.dirname(os.path.abspath(__file__))
_CONFIG = os.getenv('JITENSHEA_CONFIG')
_CONFIG = _CONFIG if _CONFIG is not None else os.path.join(_ROOT, 'config.ini')


if not os.path.isfile(_CONFIG):
    logger.error("Configuration file '%s' not found", _CONFIG)
    config = None
else:
    config = configparser.ConfigParser(allow_no_value=True)
    with open(_CONFIG) as fobj:
        config.read_file(fobj)
