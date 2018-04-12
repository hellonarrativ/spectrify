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


def _strip_schema(url):
    """Returns the url without the s3:// part"""
    result = urlparse(url)
    return result.netloc + result.path


class S3Config:
    """Describes the paths/filenames of the pertinent datafiles"""

    def fs_open(self, *args, **kwargs):
        return self.get_fs().open(*args, **kwargs)

    def get_fs(self):
        return s3fs.S3FileSystem(anon=False, default_block_size=SPECTRIFY_BLOCKSIZE)

    def get_manifest_path(self):
        return NotImplementedError('Must be implemented by subclass')

    def get_csv_dir(self):
        return NotImplementedError('Must be implemented by subclass')

    def get_spectrum_dir(self):
        return NotImplementedError('Must be implemented by subclass')


class SimpleS3Config(S3Config):
    """A simple pattern for those who dont already have a data layout"""

    @classmethod
    def from_base_path(cls, base_path, **kwargs):
        csv_dir = '/'.join([base_path, 'csv', ''])
        return cls(
            csv_dir,
            '/'.join([base_path, 'spectrum', '']),
            **kwargs
        )

    def __init__(self, csv_dir, spectrum_dir):
        self.csv_dir = csv_dir
        self.spectrum_dir = spectrum_dir

    def get_manifest_path(self):
        return self.csv_dir + 'manifest'

    def get_csv_dir(self):
        return self.csv_dir

    def get_spectrum_dir(self):
        return self.spectrum_dir


class S3GZipCSVReader:
    """Reads a Gzipped CSV file from S3
        Downloads and decompresses on-the-fly, so the entire file doesn't have
        to be loaded into memory
    """
    def __init__(self, s3_config, s3_path, **kwargs):
        self.s3file = s3_config.fs_open(_strip_schema(s3_path))
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
