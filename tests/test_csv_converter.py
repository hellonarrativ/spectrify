from io import TextIOWrapper, BytesIO
import copy
import csv
import gzip
import sys
import tempfile
import pytest

import sqlalchemy

from spectrify.convert import CsvConverter
from spectrify.utils.s3 import SimpleS3Config, S3GZipCSVReader


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


def test_columnar_data_chunks():
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
        spectrum_dir=""
    )

    csv_converter = CsvConverter(sa_table, s3_config, delimiter=delimiter, quoting=quoting)
    reader = S3GZipCSVReader(s3_config, "")
    columnar_data_chunks = [
        copy.deepcopy(row)
        for row in csv_converter.columnar_data_chunks(
            reader=reader,
            sa_table=sa_table,
            chunk_size=1
        )
    ]
    assert [
        [[1], [2], [3], [4]],
        [[1], [2], [3], [4]],
        [[1], [2], [3], [4]],
        [[1], [2], [3], [4]],
    ] == columnar_data_chunks


@pytest.mark.parametrize("file_input", [
    "file_0.csv",
    ["file_1.csv"],
    ["file_2.csv", "file_3.csv"]
])
def test_convert_csv_file(file_input):
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
        spectrum_dir=""
    )
    with tempfile.NamedTemporaryFile("wb", delete=False) as parquet_output:
        def fs_open(*args, **kwargs):
            if any(".parq" in arg for arg in args):
                return parquet_output
            else:
                s3_config._gzip_csv.seek(0)
                return s3_config._gzip_csv
        s3_config.fs_open = fs_open

        csv_converter = CsvConverter(sa_table, s3_config, delimiter=delimiter, quoting=quoting)

        csv_converter.convert_csv(file_input)
        with open(parquet_output.name, "rb") as f:
            num_lines = 0
            for _ in f:
                num_lines += 1

            assert num_lines > 0
