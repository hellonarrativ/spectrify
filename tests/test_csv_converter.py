from io import TextIOWrapper, BytesIO
from unittest import main, TestCase
import copy
import csv
import gzip
import sys

import sqlalchemy

from spectrify.convert import CsvConverter
from spectrify.utils.s3 import SimpleS3Config


def get_stream(stream):
    if sys.version_info[0] < 3:
        return stream

    return TextIOWrapper(stream, encoding='utf-8', newline='')


class FakeSimpleS3Config(SimpleS3Config):
    def __init__(
        self,
        csv_rows,
        delimiter='|',
        escapechar='\\',
        quoting=csv.QUOTE_NONE,
        *args,
        **kwargs
    ):
        SimpleS3Config.__init__(self, *args, **kwargs)
        self._gzip_csv = BytesIO()
        with get_stream(gzip.GzipFile(fileobj=self._gzip_csv, mode='wb')) as gz:
            writer = csv.writer(gz, delimiter=delimiter, escapechar=escapechar, quoting=quoting)
            for row in csv_rows:
                writer.writerow(row)

    def fs_open(self, *args, **kwargs):
        self._gzip_csv.seek(0)
        return self._gzip_csv


class TestCsvConverter(TestCase):
    def test_columnar_data_chunks(self):
        delimiter = ","
        quoting = csv.QUOTE_ALL
        sa_meta = sqlalchemy.MetaData()
        data = [
            ['1', '2', '3', '4'],
            ['1', '2', '3', '4'],
            ['1', '2', '3', '4'],
            ['1', '2', '3', '4'],
        ]
        sa_table = sqlalchemy.Table(
            'unit_test_table',
            sa_meta,
            sqlalchemy.Column('int_col_1', sqlalchemy.INTEGER),
            sqlalchemy.Column('int_col_2', sqlalchemy.INTEGER),
            sqlalchemy.Column('int_col_3', sqlalchemy.INTEGER),
            sqlalchemy.Column('int_col_4', sqlalchemy.INTEGER),
        )

        s3_config = FakeSimpleS3Config(
            data,
            delimiter=delimiter,
            quoting=quoting,
            csv_dir="",
            spectrum_dir="",
            region=""
        )
        csv_converter = CsvConverter(sa_table, s3_config, delimiter=delimiter, quoting=quoting)
        columnar_data_chunks = [
            copy.deepcopy(row)
            for row in csv_converter.columnar_data_chunks(
                data_path="",
                sa_table=sa_table,
                chunk_size=1
            )
        ]
        self.assertEqual(
            [
                [[1], [2], [3], [4]],
                [[1], [2], [3], [4]],
                [[1], [2], [3], [4]],
                [[1], [2], [3], [4]],
            ],
            columnar_data_chunks
        )


if __name__ == "__main__":
    main()
