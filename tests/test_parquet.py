from __future__ import absolute_import, division, print_function, unicode_literals
from datetime import datetime
from io import BytesIO
from unittest import TestCase

import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

from spectrify.utils.parquet import Writer


class UncloseableBytesIO(BytesIO):
    """
    pyarrow tries to close the BytesIO instance, which frees memory.
    This will trick it into thinking it has been closed...
    """

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.really_close()

    def close(self, *args, **kwargs):
        pass

    def really_close(self, *args, **kwargs):
        super(UncloseableBytesIO, self).close(*args, **kwargs)


class TestParquetWriter(TestCase):
    def setUp(self):
        self.sa_meta = sa.MetaData()
        self.data = [
            [17.124, 1.12, 3.14, 13.37],
            [1, 2, 3, 4],
            [1, 2, 3, 4],
            [1, 2, 3, 4],
            [True, None, False, True],
            ['string 1', 'string 2', None, 'string 3'],
            [datetime(2007, 7, 13, 1, 23, 34, 123456),
             None,
             datetime(2006, 1, 13, 12, 34, 56, 432539),
             datetime(2010, 8, 13, 5, 46, 57, 437699), ],
            ["Test Text", "Some#More#Test#  Text", "!@#$%%^&*&", None],
        ]
        self.table = sa.Table(
            'unit_test_table',
            self.sa_meta,
            sa.Column('real_col', sa.REAL),
            sa.Column('bigint_col', sa.BIGINT),
            sa.Column('int_col', sa.INTEGER),
            sa.Column('smallint_col', sa.SMALLINT),
            sa.Column('bool_col', sa.BOOLEAN),
            sa.Column('str_col', sa.VARCHAR),
            sa.Column('timestamp_col', sa.TIMESTAMP),
            sa.Column('plaintext_col', sa.TEXT),
        )

        self.expected_datatypes = [
            pa.float32(),
            pa.int64(),
            pa.int32(),
            pa.int16(),
            pa.bool_(),
            pa.string(),
            pa.timestamp('ns'),
            pa.string(),
        ]

    def test_write(self):
        # Write out test file
        with UncloseableBytesIO() as write_buffer:
            with Writer(write_buffer, self.table) as writer:
                writer.write_row_group(self.data)
            file_bytes = write_buffer.getvalue()

        # Read in test file
        read_buffer = BytesIO(file_bytes)
        with pa.PythonFile(read_buffer, mode='r') as infile:

            # Verify data
            parq_table = pq.read_table(infile)
            written_data = list(parq_table.to_pydict().values())

            tuples_by_data_type = zip(self.data, written_data)
            for i in tuples_by_data_type:
                tuples_by_order = zip(i[0], i[1])
                for j in tuples_by_order:
                    self.assertAlmostEquals(j[0], j[1], places=5)

            # Verify parquet file schema
            for i, field in enumerate(parq_table.schema):
                self.assertEqual(field.type.id, self.expected_datatypes[i].id)

            # Ensure timestamp column was written with int96; right now
            # there is no way to see except to check that the unit on
            # the timestamp type is 'ns'
            ts_col = parq_table.schema.field_by_name('timestamp_col')
            self.assertEqual(ts_col.type.unit, 'ns')
