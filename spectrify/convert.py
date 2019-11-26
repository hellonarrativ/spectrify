from __future__ import absolute_import, division, print_function
from future.standard_library import install_aliases
install_aliases()  # noqa
from builtins import list

import sys
import csv
import gc
import json
from datetime import datetime, date
from decimal import Decimal, Context, setcontext
from os import path, environ, getenv
from multiprocessing import Pool, cpu_count

import click
from spectrify.utils.timestamps import iso8601_to_nanos, iso8601_to_days_since_epoch
from spectrify.utils.parquet import Writer
from spectrify.utils.s3 import S3GZipCSVReader

# Redshift allows up to 38 bits of decimal/numeric precision. Set the Python
# decimal context accordingly
redshift_context = Context(prec=38)
setcontext(redshift_context)

# This determines the number of rows in each row group of the Parquet file.
# Larger row group means better compression.
# Larger row group also means more memory required for write.
# Assuming a row size of 2KB, this will process approx. 500MB of data per group.
# Actual memory usage will be some multiple of that, since multiple copies
# are required in memory for processing.
SPECTRIFY_ROWS_PER_GROUP = environ.get('SPECTRIFY_ROWS_PER_GROUP') or 250000

# Python2 csv builtin library has limited support with unicode CSVs
# (see: https://github.com/hellonarrativ/spectrify/issues/16).
# Therefore, there is an option to replace the builtin csv module with `unicodecsv` module.
# By default this option is disabled - `unicodecsv` can have a pretty serious performance
# impact.
SPECTRIFY_USE_UNICODE_CSV = bool(getenv("SPECTRIFY_USE_UNICODE_CSV")) or False

# These are the values Redshift uses for true/false in its CSVs
POSTGRES_TRUE_VAL = 't'
POSTGRES_FALSE_VAL = 'f'


def postgres_bool_to_python_bool(val):
    """Redshift CSV exports use postgres-style 't'/'f' values for boolean
        columns. This function parses the string and returns a python bool
    """
    if val:
        if val == POSTGRES_TRUE_VAL:
            return True
        elif val == POSTGRES_FALSE_VAL:
            return False
        else:
            raise ValueError("Unknown boolean value {}".format(val))
    return None


""" The CSV reader passes in strings, and we want to convert them to various
Arrow/Parquet types.  Unfortunately Arrow doesn't know how to convert from
string directly to those types.  The functions below will convert a string
to an appropriate Python type, such that it can be parsed into the corresponding
Arrow type.
"""
string_converters = {
    int: int,
    float: float,
    bool: postgres_bool_to_python_bool,
    Decimal: Decimal,
    datetime: iso8601_to_nanos,
    date: iso8601_to_days_since_epoch,  # Actually converts to int via datetime!
}

if sys.version_info[0] < 3:
    string_converters.update({
        int: long,
        long: long,
        str: unicode,
    })


class CsvConverter:
    def __init__(self, sa_table, s3_config, delimiter='|', escapechar='\\', quoting=csv.QUOTE_NONE,
                 unicode_csv=SPECTRIFY_USE_UNICODE_CSV, **kwargs):
        self.sa_table = sa_table
        self.s3_config = s3_config
        self.delimiter = delimiter
        self.escapechar = escapechar
        self.quoting = quoting
        self.unicode_csv = unicode_csv
        self.kwargs = kwargs

    def log(self, msg):
        """By default, we log to console with click"""
        click.echo(msg)

    def get_manifest(self):
        with self.s3_config.fs_open(self.s3_config.get_manifest_path()) as manifest_file:
            return json.loads(manifest_file.read().decode('utf-8'))

    def convert_csv(self, file_path):
        """Converts an individual datafile on S3 to parquet"""
        filename, ext = path.splitext(path.basename(file_path))
        out_dir = self.s3_config.get_spectrum_dir()
        out_path = path.join(out_dir, filename)
        if not out_path.endswith('.parq'):
            out_path += '.parq'

        self.log('Converting file [%s] to [%s]' % (file_path, out_path))

        with self.s3_config.fs_open(out_path, 'wb') as s3_file:
            with Writer(s3_file, self.sa_table) as writer:
                # Read the data in chunks (to control memory usage) and write to parquet.
                # The obvious choice is to use Pandas for this, but issues with null values and
                # difficulty with type conversions were a blocker when I originally wrote this code.
                # It's possible the situation has evolved.
                #
                # Assuming those issues have solutions, using Pandas would probably be much more
                # efficient in terms of CPU and memory.
                for chunk in self.columnar_data_chunks(file_path, self.sa_table, SPECTRIFY_ROWS_PER_GROUP):
                    writer.write_row_group(chunk)

        self.log('Done converting file [%s] to [%s]' % (file_path, out_path))

    def _clear_and_collect(self, data):
        for col in data:
            col.clear()

        # Explicitly run garbage collector; previous data isn't needed anymore
        # and will just add unnecessary memory pressure until python decides
        # it's worth running
        gc.collect()

    def _convert_to_type(self, value, py_type):
        """Converts the CSV string element to an intermediary python datatype
        This is necessary because Arrow can't parse strings itself, it expects
        an intermediary data type (actual type is determined by the destination
        arrow datatype)
        """
        if value == '':
            value = None
        elif py_type:
            value = py_type(value)
        return value

    def columnar_data_chunks(self, data_path, sa_table, chunk_size):
        """A generator function that returns chunk_size rows (or whatever is left
        at the end of the file) in columnar format
        This function also performs conversion from string to python datatype based on the given
        SQLAlchemy schema.
        """

        # An array of functions corresponding to the CSV columns that take a string and return the
        # corresponding Python datatype
        type_converters = self.table_to_conversion_funcs(sa_table)
        with self.get_csv_reader(data_path) as reader:
            num_cols = len(sa_table.columns)
            col_indices = range(num_cols)
            data = [list() for i in range(num_cols)]

            # Read in CSV and store it by column (makes passing to Arrow easier)
            for row in reader:
                # read a row
                for i in col_indices:
                    value = row[i]
                    py_type = type_converters[i]

                    value = self._convert_to_type(value, py_type)
                    data[i].append(value)

                if len(data[0]) == chunk_size:
                    yield data
                    self._clear_and_collect(data)

            # Number of rows in file is not necessarily divisible by chunk_size
            # So make sure there isn't any lingering data to process
            if data[0]:
                yield data
                self._clear_and_collect(data)

    def table_to_conversion_funcs(self, sa_table):
        cols = sa_table.columns
        return [string_converters.get(col.type.python_type) for col in cols]

    def get_csv_reader(self, data_path):
        return S3GZipCSVReader(
            self.s3_config,
            data_path,
            delimiter=self.delimiter,
            escapechar=self.escapechar,
            quoting=self.quoting,
            unicode_csv=self.unicode_csv
        )


class _PoolManager(object):
    """Pool in Python 2 doesn't act as a context manager. So just make one here"""

    def __init__(self, *args, **kwargs):
        self.pool_args = args
        self.pool_kwargs = kwargs
        self.pool = None

    def __enter__(self):
        self.pool = Pool(*(self.pool_args), **(self.pool_kwargs))
        return self.pool

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.close()
        self.pool.join()


def _parallel_wrapper(arg_tuple):
    data_path, sa_table, s3_config, delimiter, escapechar, quoting, unicode_csv = arg_tuple
    CsvConverter(sa_table, s3_config, delimiter, escapechar, quoting, unicode_csv).convert_csv(data_path)


class ConcurrentManifestConverter(CsvConverter):
    """Converts CSV files concurrently using a multiprocessing pool."""

    def convert_manifest(self):
        num_workers = self.kwargs.get('num_workers') or cpu_count()
        manifest = self.get_manifest()
        convert_args = [
            (
                entry['url'], self.sa_table, self.s3_config, self.delimiter,
                self.escapechar, self.quoting, self.unicode_csv
            )
            for entry in manifest['entries']
        ]

        with _PoolManager(num_workers) as pool:
            pool.map(_parallel_wrapper, convert_args, chunksize=1)


class SimpleManifestConverter(CsvConverter):
    def convert_manifest(self):
        manifest = self.get_manifest()
        for entry in manifest['entries']:
            self.convert_csv(entry['url'])
