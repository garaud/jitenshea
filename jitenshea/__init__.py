# coding: utf-8

"""Jitenshea package

Bicycle-sharing analysis
"""

import os
import logging
import configparser


FORMAT = '%(asctime)s :: %(levelname)s :: %(funcName)s : %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.abspath(__file__))
_CONFIG = os.getenv('JITENSHEA_CONFIG')
_CONFIG = _CONFIG if _CONFIG is not None else os.path.join(_ROOT, 'config.ini')


if not os.path.isfile(_CONFIG):
    logger.warning("Configuration file '%s' not found", _CONFIG)
    config = None
else:
    config = configparser.ConfigParser(allow_no_value=True)
    with open(_CONFIG) as fobj:
        config.read_file(fobj)
