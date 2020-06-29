#!/usr/bin/python3.6

from setuptools import setup, find_packages
setup(
    name = 'gnaf_loader',
    packages = find_packages(exclude=['tests.*']),
    version = '0.1',
    description = 'GNAF loader',
    author = 'William Librata',
    author_email = 'william.librata@gmail.com',
    url = 'https://librata.io',
    entry_points={
        'console_scripts': [
            'gnaf_loader=gnaf_loader.gnaf_loader:main',
        ],
    },
)


