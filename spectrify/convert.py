from __future__ import absolute_import, division, print_function
from future.standard_library import install_aliases
install_aliases()  # noqa
from builtins import list

import sys
import csv
import gc
import json
from datetime import datetime
from os import path, environ
from multiprocessing import Pool, cpu_count

import click
from spectrify.utils.timestamps import iso8601_to_nanos
from spectrify.utils.parquet import Writer
from spectrify.utils.s3 import get_fs, S3GZipCSVReader

# This determines the number of rows in each row group of the Parquet file.
# Larger row group means better compression.
# Larger row group also means more memory required for write.
# Assuming a row size of 2KB, this will process approx. 500MB of data per group.
# Actual memory usage will be some multiple of that, since multiple copies
# are required in memory for processing.
SPECTRIFY_ROWS_PER_GROUP = environ.get('SPECTRIFY_ROWS_PER_GROUP') or 250000


class PoolManager(object):
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


def convert_redshift_manifest_to_parquet(manifest_path, sa_table, out_dir, workers=None):
    """Parses manifest file and then converts each referenced file"""

    with get_fs().open(manifest_path) as manifest_file:
        manifest = json.loads(manifest_file.read().decode('utf-8'))
    num_files = len(manifest['entries'])

    workers = workers or min(num_files, cpu_count())
    click.echo('Converting {} csv files to parquet with {} workers.'.format(num_files, workers))

    if workers > 1:
        convert_parallel(manifest, sa_table, out_dir, workers)
    else:
        convert_synchronous(manifest, sa_table, out_dir)


def convert_parallel(manifest, sa_table, out_dir, workers):
    convert_args = [(entry['url'], sa_table, out_dir) for entry in manifest['entries']]

    with PoolManager(workers) as pool:
        pool.map(_parallel_wrapper, convert_args, chunksize=1)


def _parallel_wrapper(arg_tuple):
    data_path, sa_table, out_dir = arg_tuple
    convert_csv_file_to_parquet(data_path, sa_table, out_dir)


def convert_synchronous(manifest, sa_table, out_dir):
    for entry in manifest['entries']:
        convert_csv_file_to_parquet(entry['url'], sa_table, out_dir)


def convert_csv_file_to_parquet(data_path, sa_table, out_dir):
    """Converts an individual datafile on S3 to parquet"""
    filename, ext = path.splitext(path.basename(data_path))
    out_path = path.join(out_dir, filename)
    if not out_path.endswith('.parq'):
        out_path += '.parq'

    click.echo('Converting file [%s] to [%s]' % (data_path, out_path))

    with get_fs().open(out_path, 'wb') as s3_file:
        with Writer(s3_file, sa_table) as writer:

            # Read the data in chunks (to control memory usage) and write to parquet.
            # The obvious choice is to use Pandas for this, but issues with null values and
            # difficulty with type conversions were a blocker when I originally wrote this code.
            # It's possible the situation has evolved.
            #
            # Assuming those issues have solutions, using Pandas would probably be much more
            # efficient in terms of CPU and memory.
            for chunk in columnar_data_chunks(data_path, sa_table, SPECTRIFY_ROWS_PER_GROUP):
                writer.write_row_group(chunk)

    click.echo('Done converting file [%s] to [%s]' % (data_path, out_path))


def _clear_and_collect(data):
    for col in data:
        col.clear()

    # Explicitly run garbage collector; previous data isn't needed anymore
    # and will just add unnecessary memory pressure until python decides
    # it's worth running
    gc.collect()


def _convert_to_type(value, py_type):
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


def columnar_data_chunks(data_path, sa_table, chunk_size):
    """A generator function that returns chunk_size rows (or whatever is left
    at the end of the file) in columnar format
    This function also performs conversion from string to python datatype based on the given
    SQLAlchemy schema.
    """

    # An array of functions corresponding to the CSV columns that take a string and return the
    # corresponding Python datatype
    type_converters = table_to_conversion_funcs(sa_table)

    with S3GZipCSVReader(data_path, delimiter='|', escapechar='\\', quoting=csv.QUOTE_NONE) as reader:
        num_cols = len(sa_table.columns)
        col_indices = range(num_cols)
        data = [list() for i in range(num_cols)]

        # Read in CSV and store it by column (makes passing to Arrow easier)
        for row in reader:
            # read a row
            for i in col_indices:
                value = row[i]
                py_type = type_converters[i]

                value = _convert_to_type(value, py_type)
                data[i].append(value)

            if len(data[0]) == chunk_size:
                yield data
                _clear_and_collect(data)

        # Number of rows in file is not necessarily divisible by chunk_size
        # So make sure there isn't any lingering data to process
        if data[0]:
            yield data
            _clear_and_collect(data)


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
    datetime: iso8601_to_nanos,
}

if sys.version_info[0] < 3:
    string_converters.update({
        int: long,
        long: long,
        str: unicode,
    })


def table_to_conversion_funcs(sa_table):
    cols = sa_table.columns
    return [string_converters.get(col.type.python_type) for col in cols]
