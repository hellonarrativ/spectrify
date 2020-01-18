=========
Spectrify
=========


.. image:: https://img.shields.io/pypi/v/spectrify.svg
    :target: https://pypi.python.org/pypi/spectrify

.. image:: https://img.shields.io/travis/hellonarrativ/spectrify.svg
    :target: https://travis-ci.org/hellonarrativ/spectrify

.. image:: https://readthedocs.org/projects/spectrify/badge/?version=latest
    :target: https://spectrify.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


A simple yet powerful tool to move your data from Redshift to Redshift Spectrum.


* Free software: MIT license
* Documentation: https://spectrify.readthedocs.io.


Features
--------

One-liners to:

* Export a Redshift table to S3 (CSV)
* Convert exported CSVs to Parquet files in parallel
* Create the Spectrum table on your Redshift cluster
* **Perform all 3 steps in sequence**, essentially "copying" a Redshift table Spectrum in one command.

S3 credentials are specified using boto3. See http://boto3.readthedocs.io/en/latest/guide/configuration.html

Redshift credentials are supplied via environment variables, command-line parameters, or interactive prompt.

Install
--------

.. code-block:: bash

    $ pip install psycopg2  # or psycopg2-binary
    $ pip install spectrify


Command-line Usage
------------------

Export Redshift table `my_table` to a folder of CSV files on S3:

.. code-block:: bash

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb export my_table \
        's3://example-bucket/my_table'

Convert exported CSVs to Parquet:

.. code-block:: bash

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb convert my_table \
        's3://example-bucket/my_table'

Create Spectrum table from S3 folder:

.. code-block:: bash

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb create_table \
        's3://example-bucket/my_table' my_table my_spectrum_table

Transform Redshift table by performing all 3 steps in sequence:

.. code-block:: bash

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb transform my_table \
        's3://example-bucket/my_table'


Python Usage
------------

Export to S3:

.. code-block:: python


    from spectrify.export import RedshiftDataExporter
    RedshiftDataExporter(sa_engine, s3_config).export_to_csv('my_table')

Convert exported CSVs to Parquet:

.. code-block:: python

    from spectrify.convert import ConcurrentManifestConverter
    from spectrify.utils.schema import SqlAlchemySchemaReader
    sa_table = SqlAlchemySchemaReader(engine).get_table_schema('my_table')
    ConcurrentManifestConverter(sa_table, s3_config).convert_manifest()

Create Spectrum table from S3 parquet folder:

.. code-block:: python

    from spectrify.create import SpectrumTableCreator
    from spectrify.utils.schema import SqlAlchemySchemaReader
    sa_table = SqlAlchemySchemaReader(engine).get_table_schema('my_table')
    SpectrumTableCreator(sa_engine, dest_schema, dest_table_name, sa_table, s3_config).create()

Transform Redshift table by performing all 3 steps in sequence:

.. code-block:: python

    from spectrify.transform import TableTransformer
    transformer = TableTransformer(engine, 'my_table', s3_config, dest_schema, dest_table_name)
    transformer.transform()

Contribute
----------
Contributions always welcome! Read our guide on contributing here: http://spectrify.readthedocs.io/en/latest/contributing.html

License
-------
MIT License. Copyright (c) 2017, The Narrativ Company, Inc.
