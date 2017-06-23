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
CONFIG_FILE = os.path.join(_ROOT, 'config.ini')


if not os.path.isfile(CONFIG_FILE):
    logger.warning("Configuration file '%s' not found", CONFIG_FILE)
    config = None
else:
    config = configparser.ConfigParser(allow_no_value=True)
    with open(CONFIG_FILE) as fobj:
        config.read_file(fobj)
