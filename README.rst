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

    $ pip install spectrify


Command-line Usage
------------------

Export Redshift table `my_table` to a folder of CSV files on S3:

.. code-block::

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb export my_table \
        's3://example-bucket/my_table'

Convert exported CSVs to Parquet:

.. code-block::

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb convert my_table \
        's3://example-bucket/my_table'

Create Spectrum table from S3 folder:

.. code-block::

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb create_table \
        's3://example-bucket/my_table' my_table my_spectrum_table

Transform Redshift table by performing all 3 steps in sequence:

.. code-block::

    $ spectrify --host=example-url.redshift.aws.com --user=myuser --db=mydb transform my_table \
        's3://example-bucket/my_table'


Python Usage
------------

Currently, you'll have to supply your own SQL Alchemy engine to each of the below commands (pull requests welcome to make this eaiser).

Export to S3:

.. code-block:: python

    from spectrify.export import export_to_csv
    export_to_csv(sa_engine, table_name, s3_csv_dir)

Convert exported CSVs to Parquet:

.. code-block:: python

    from spectrify.convert import convert_redshift_manifest_to_parquet
    from spectrify.utils.schema import get_table_schema
    sa_table = get_table_schema(sa_engine, source_table_name)
    convert_redshift_manifest_to_parquet(s3_csv_manifest_path, sa_table, s3_spectrum_dir)

Create Spectrum table from S3 parquet folder:

.. code-block:: python

    from spectrify.create import create_external_table
    from spectrify.utils.schema import get_table_schema
    sa_table = get_table_schema(sa_engine, source_table_name)
    create_external_table(sa_engine, dest_schema, dest_table_name, sa_table, s3_spectrum_path)

Transform Redshift table by performing all 3 steps in sequence:

.. code-block:: python

    from spectrify.transform import transform_table
    transform_table(sa_engine, table_name, s3_base_path, dest_schema, dest_table, num_workers)

Contribute
----------
Contributions always welcome! Read our guide on contributing here: http://spectrify.readthedocs.io/en/latest/contributing.html

License
-------
MIT License. Copyright (c) 2017, The Narrativ Company, Inc.
