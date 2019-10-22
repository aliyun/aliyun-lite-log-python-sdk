#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

"""Setup script for log service of Kafka and ElasticSearch plugin SDK.

Depending on your version of Python, these libraries may also should be installed:
http://pypi.python.org/pypi/simplejson/

"""

try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

import sys
import re

install_requires_common = ['responses']
install_requires = []

# if sys.version_info[:2] == (2, 6):
# 	install_requires = ['aliyun-log-python-sdk>=0.6.44', 'elasticsearch>=6.0.0,<7.0.0', 'kafka-python>=1.4.4']
if sys.version_info[0] == 2:
	install_requires = ['aliyun-log-python-sdk>=0.6.44', 'elasticsearch>=6.0.0,<7.0.0', 'kafka-python>=1.4.4']
# elif sys.version_info[:2] == (3, 3):
# 	install_requires = ['aliyun-log-python-sdk>=0.6.44', 'elasticsearch>=6.0.0,<7.0.0', 'kafka-python>=1.4.4']
elif sys.version_info[0] == 3:
	# install_requires = ['aliyun-log-python-sdk>=0.6.44', 'elasticsearch>=6.0.0,<7.0.0', 'kafka-python>=1.4.4']
	raise Exception('Not supported Python version, please use Python 2.')

install_requires.extend(install_requires_common)

packages = [
	'loglite',
	'loglite.log',
]

version = ''
with open('loglite/log/version.py', 'r') as fd:
	version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
		fd.read(), re.MULTILINE).group(1)

classifiers = [
	'Development Status :: 5 - Production/Stable',
	'License :: OSI Approved :: MIT License',
	'Operating System :: OS Independent',
	'Programming Language :: Python :: 2.6',
	'Programming Language :: Python :: 2.7',
	# 'Programming Language :: Python :: 3.3',
	# 'Programming Language :: Python :: 3.4',
	# 'Programming Language :: Python :: 3.5',
	# 'Programming Language :: Python :: 3.6',
	# 'Programming Language :: Python :: 3.7',
	'Programming Language :: Python :: Implementation :: PyPy'
]

long_description = """
Python SDK for Aliyun Lite Log
http://aliyun-lite-log-python-sdk.readthedocs.io
"""

setup(
	name='aliyun-lite-log-python-sdk',
	version=version,
	description='Aliyun log Kafka ElasticSearch Plugin Python client SDK',
	author='Aliyun',
	url='https://github.com/aliyun/aliyun-lite-log-python-sdk',
	install_requires=install_requires,
	packages=packages,
	classifiers=classifiers,
	long_description=long_description
)
