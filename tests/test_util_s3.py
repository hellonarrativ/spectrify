# -*- coding: utf8 -*-
from unittest import main, TestCase
import gzip
import tempfile

from spectrify.utils.s3 import S3GZipCSVReader, get_csv_reader


class FakeS3Config(object):
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def fs_open(self, *args, **kwargs):
        return self.fileobj


class TestUtilsS3CSVReader(TestCase):
    def setUp(self):
        self.unicode_csv_lines = [
            u"name,age,sex",
            u"Nir,31,M",
            u"ניר,31,M"
        ]
        self.encoded_csv_lines = [
            ['name', 'age', 'sex'],
            ['Nir', '31', 'M'],
            ['ניר', '31', 'M']
        ]

    def test_get_csv_reader(self):
        self.assertEquals(
            self.encoded_csv_lines,
            list(get_csv_reader(self.unicode_csv_lines))
        )

    def test_s3_gzip_csv_reader(self):
        gzip_csv = tempfile.TemporaryFile()
        with gzip.GzipFile(fileobj=gzip_csv, mode="wb") as _gzip:
            _gzip.write("\n".join(self.unicode_csv_lines).encode('utf-8'))

        gzip_csv.seek(0)

        fake_s3_config = FakeS3Config(gzip_csv)
        self.assertEquals(
            self.encoded_csv_lines,
            list(S3GZipCSVReader(fake_s3_config, ""))
        )


if __name__ == "__main__":
    main()
