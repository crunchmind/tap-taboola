#!/usr/bin/env python

from setuptools import setup

setup(name='tap-taboola',
      version='0.3.1',
      description='Singer.io tap for extracting data from the Taboola API',
      author='Fishtown Analytics',
      url='http://www.singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_taboola'],
      install_requires=[
          'singer-python==5.12.2',
          'backoff==1.8.0',
          'requests==2.20.0',
          'python-dateutil==2.6.0'
      ],
      entry_points='''
          [console_scripts]
          tap-taboola=tap_taboola:main
      ''',
      packages=['tap_taboola'],
      package_data={
          'tap_taboola/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True
      )
