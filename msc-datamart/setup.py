#!/usr/bin/env python

from distutils.core import setup

setup(name='buoy_data_ingest',
      version='1.0',
      description='SWOB-ML buoy data to database',
      author='Nate Rosenstock',
      author_email='nate.rosenstock@hakai.org',
      packages=['buoy_data_ingest'],
      url='',
      install_requires=['metpx-sarracenia',
                        'psycopg2-binary',
                        'xmltodict',
                        'sqlalchemy',
                        'paramiko'],
      )
