# -*- coding: utf8 -*-
from unittest import main, TestCase
import gzip
import tempfile

import unicodecsv as csv

from spectrify.utils.s3 import S3GZipCSVReader


class FakeS3Config(object):
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def fs_open(self, *args, **kwargs):
        self.fileobj.seek(0)
        return self.fileobj


class TestUtilsS3CSVReader(TestCase):
    def test_s3_gzip_csv_reader(self):
        encoded_csv_lines = [
            [u'name', u'age', u'sex'],
            [u'Nir', u'31', u'M'],
            [u'ניר', u'31', u'M'],
            [u"Martin von Löwis", u'31', u'M'],
            [u"Marc André Lemburg", u'31', u'M'],
            [u"François Pinard", u'31', u'M']
        ]
        gzip_csv = tempfile.TemporaryFile()
        with gzip.GzipFile(fileobj=gzip_csv, mode="wb") as _gzip:
            csv_writer = csv.writer(_gzip)
            csv_writer.writerows(encoded_csv_lines)

        fake_s3_config = FakeS3Config(gzip_csv)
        with S3GZipCSVReader(fake_s3_config, "", unicode_csv=True) as s3_gzip_csv_reader:
            self.assertEqual(encoded_csv_lines, list(s3_gzip_csv_reader))


if __name__ == "__main__":
    main()
