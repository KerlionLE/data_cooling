#!/usr/bin/env python
from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_description = fh.read()

__version__ = '1.0.0'
APP_NAME = 'af_project_part'

setup(
    name=APP_NAME,
    version=__version__,
    author='Samokhin Ivan',
    author_email='samokhinia@sibur.ru',
    description='some part of airflow',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=['setuptools==65.5.1'],
    extras_require={},
    tests_require=[],
    entry_points={},
    classifiers=[
        'Programming Language :: Python :: ',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independen',
    ],
)
