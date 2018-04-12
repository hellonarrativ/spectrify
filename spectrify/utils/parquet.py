import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP


def _pa_timestamp_ns():
    """Wrapper function around Arrow's timestamp type function, which is the
    only type function that requires an argument...
    """
    return pa.timestamp('ns')


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
    DOUBLE_PRECISION: pa.float64,
    sa.types.VARCHAR: pa.string,
    sa.types.NVARCHAR: pa.string,
    sa.types.CHAR: pa.string,
    sa.types.BOOLEAN: pa.bool_,
    sa.types.TIMESTAMP: _pa_timestamp_ns,
    TIMESTAMP: _pa_timestamp_ns,
}


class Writer:
    """Holds onto the Arrow write manager and appropriately deals with
        closing when finished or upon an error
    """
    def __init__(self, py_fd, sa_table):
        cols = sa_table.columns
        self.py_fd = py_fd
        self.col_types = [pyarrow_type_map[col.type.__class__] for col in cols]
        self.col_names = [col.description for col in cols]
        self.writer = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.writer:
            self.writer.close()

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
