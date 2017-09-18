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
    'psycopg2',
    'pyarrow==0.5.0.post2',
    's3fs',
    'SQLAlchemy',
    'sqlalchemy-redshift',
]

setup_requirements = [
    'pytest-runner',
    # TODO(hellonarrativ): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'pytest',
    'pandas',  # Needed for reading in timestamp columns
]

setup(
    name='spectrify',
    version='0.1.0',
    description="Tools for working with Redshift Spectrum.",
    long_description=readme + '\n\n' + history,
    author="Narrativ, Inc",
    author_email='engineering@narrativ.com',
    url='https://github.com/hellonarrativ/spectrify',
    packages=find_packages(include=['spectrify']),
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
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
