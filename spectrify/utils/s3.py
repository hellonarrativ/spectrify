from __future__ import absolute_import, division, print_function
from future.standard_library import install_aliases
install_aliases()  # noqa

import csv
import sys
from gzip import GzipFile
from io import TextIOWrapper
from urllib.parse import urlparse

import s3fs

SPECTRIFY_BLOCKSIZE = 50 * 2**20  # 50MB

# https://bugs.python.org/issue12591
if sys.version_info[0] < 3:
    class HackedGzipFile(GzipFile):
        def read1(self, n):
            return self.read(n)

    GzipFile = HackedGzipFile


def get_fs():
    return s3fs.S3FileSystem(anon=False, default_block_size=SPECTRIFY_BLOCKSIZE)


def strip_schema(url):
    """Returns the url without the s3:// part"""
    result = urlparse(url)
    return result.netloc + result.path


def paths_from_base_path(s3_base_path):
    # Don't use path, since it might use backlashes on windows.
    # S3 always wants forward slashes
    s3_csv_path = '/'.join([s3_base_path, 'csv', ''])
    s3_csv_manifest = s3_csv_path + 'manifest'
    s3_spectrum_path = '/'.join([s3_base_path, 'spectrum', ''])
    return s3_csv_path, s3_csv_manifest, s3_spectrum_path


class S3GZipCSVReader:
    """Reads a Gzipped CSV file from S3
        Downloads and decompresses on-the-fly, so the entire file doesn't have
        to be loaded into memory
    """
    def __init__(self, s3_path, **kwargs):
        s3 = get_fs()
        self.s3file = s3.open(strip_schema(s3_path))
        self.gzfile = TextIOWrapper(
            GzipFile(fileobj=self.s3file, mode='rb'),
            encoding='utf-8',
            newline='',
        )
        self.reader = csv.reader(self.gzfile, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self.reader.__iter__()

    def next(self):
        return self.reader.next()

    def close(self):
        self.gzfile.close()
        self.s3file.close()
