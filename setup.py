#!/usr/bin/env python3
from distutils.core import setup

version='0.9'

setup(
    name='schedcaster',
    version=version,
    author='Artem Shitov',
    author_email='mail@artem-shitov.com',

    packages=['schedcaster'],

    url='https://github.com/shitov/schedcaster',
    license = 'MIT license',
    description = "lib for posting to VK on a schedule",

    long_description = "lib for posting to VK on a schedule",

    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
