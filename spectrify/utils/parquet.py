import functools
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP


def _pa_timestamp_ns():
    """Wrapper function around Arrow's timestamp type function, which is the
    only type function that requires an argument...
    """
    return pa.timestamp('ns')


class Writer:
    """Writes a Parquet file using Apache Arrow"""

    # A mapping of SqlAlchemy types to Pyarrow types.
    # Spectrify uses the following schema conversion strategy:
    #
    #   Redshift Table Schema (1) --> SqlAlchemy Schema (2) --> Pyarrow Schema (3) --> Parquet (4)
    #
    # The following mapping determines how to go from (2) to (3).
    pyarrow_type_map = {
        sa.types.BIGINT: pa.int64,
        sa.types.INTEGER: pa.int32,
        sa.types.SMALLINT: pa.int16,
        sa.types.FLOAT: pa.float64,
        sa.types.REAL: pa.float32,
        DOUBLE_PRECISION: pa.float64,
        sa.types.VARCHAR: pa.string,
        sa.types.NVARCHAR: pa.string,
        sa.types.CHAR: pa.string,
        sa.types.BOOLEAN: pa.bool_,
        sa.types.TIMESTAMP: _pa_timestamp_ns,
        sa.types.DATE: pa.date32,
        sa.types.TEXT: pa.string,
        TIMESTAMP: _pa_timestamp_ns,
    }
    supported_sa_types = set(pyarrow_type_map.keys()).union({sa.types.DECIMAL, sa.types.NUMERIC})

    def __init__(self, py_fd, sa_table):
        cols = sa_table.columns
        self.py_fd = py_fd
        self.col_types = self.determine_pyarrow_types(cols)
        self.col_names = [col.description for col in cols]
        self.writer = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.writer:
            self.writer.close()

    def determine_pyarrow_types(self, cols):
        pa_types = []
        for col in cols:
            sa_class = col.type.__class__
            if isinstance(col.type, (sa.types.NUMERIC, sa.types.DECIMAL)):
                pa_type = functools.partial(pa.decimal128, col.type.precision, col.type.scale)
            else:
                pa_type = self.pyarrow_type_map[sa_class]
            pa_types.append(pa_type)
        return pa_types

    def write_row_group(self, cols):
        """ Write rows (stored in columnar lists) to Parquet file"""
        arrays = self._to_arrow_arrays(cols)
        table = pa.Table.from_arrays(arrays, self.col_names)

        # Writer has to be created here because we need a table
        # Assumes that data passed in will always have the same columns for
        # calls to a single Writer instance
        writer = self._get_writer(table)
        writer.write_table(table)

    def _to_arrow_arrays(self, cols):
        """Create arrow arrays from intermediary Python columnar data"""
        arrays = []

        # Sanity check that the first and last columns are the same length
        assert len(cols[0]) == len(cols[-1])

        for i in range(len(self.col_types)):
            arrow_type_func = self.col_types[i]
            arrow_type = arrow_type_func()
            arr = pa.array(cols[i], arrow_type)
            arrays.append(arr)

        return arrays

    def _get_writer(self, table):
        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.py_fd,
                table.schema,
                compression='gzip',
                use_deprecated_int96_timestamps=True
            )
        return self.writer
