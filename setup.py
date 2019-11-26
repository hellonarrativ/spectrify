#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'boto3',
    'ciso8601',
    'Click',
    'future',
    'pandas',
    'psycopg2',
    'pyarrow>=0.9.0',
    'python-dateutil<2.7.0,>=2.1',
    's3fs',
    'sqlalchemy',
    'sqlalchemy-redshift>=0.7.1',
    'unicodecsv'
]

setup_requirements = [
    'pytest-runner',
    # TODO(hellonarrativ): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'pytest',
]

setup(
    name='spectrify',
    version='3.0.1',
    description="Tools for working with Redshift Spectrum.",
    long_description=readme + '\n\n' + history,
    author="The Narrativ Company, Inc.",
    author_email='engineering@narrativ.com',
    url='https://github.com/hellonarrativ/spectrify',
    packages=find_packages(include=['spectrify', 'spectrify.*']),
    entry_points={
        'console_scripts': [
            'spectrify=spectrify.main:cli'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    dependency_links=[
        'git+https://github.com/sqlalchemy-redshift/sqlalchemy-redshift.git@7f6d2bff2d9e90afb04c1df954d2864d49275941#egg=sqlalchemy-redshift',
    ],
    license="MIT license",
    zip_safe=False,
    keywords='spectrify',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
