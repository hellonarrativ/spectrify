import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from pyarrow.lib import TimestampType
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP


def pa_timestamp_ns():
    """Wrapper function around Arrow's timestamp type function, which is the
    only type function that requires an argument...
    """
    return pa.timestamp('ns')


# Map between SqlAlchemy type classes and pyarrow/parquet type generator functions.
sa_type_map = {
    sa.types.BIGINT: pa.int64,
    sa.types.INTEGER: pa.int32,
    sa.types.SMALLINT: pa.int32,  # 32 bits to be on the safe side...
    sa.types.FLOAT: pa.float64,
    DOUBLE_PRECISION: pa.float64,
    sa.types.VARCHAR: pa.string,
    sa.types.NVARCHAR: pa.string,
    sa.types.CHAR: pa.string,
    sa.types.BOOLEAN: pa.bool_,
    sa.types.TIMESTAMP: pa_timestamp_ns,
    TIMESTAMP: pa_timestamp_ns,
}

# pyarrow only supports 64-bit ints right now, so turn everything into int64's
# This is obviously inefficient across a number of dimensions...
# TODO: PR to pyarrow to support narrower ints
unsupported_int_types = {pa.int8, pa.int16, pa.int32}
for sa_type, arrow_type in sa_type_map.items():
    if arrow_type in unsupported_int_types:
        sa_type_map[sa_type] = pa.int64


class Writer:
    """Holds onto the Arrow write manager and appropriately deals with
        closing when finished or upon an error
    """
    def __init__(self, py_fd, sa_table):
        cols = sa_table.columns
        self.py_fd = py_fd
        self.col_types = [sa_type_map[col.type.__class__] for col in cols]
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
            if isinstance(arrow_type, TimestampType) and arrow_type.unit == 'ns':
                # Currently the only way to get a pyarrow array of nanosecond
                # timestamps is via pandas (well, technically numpy).
                np_arr = np.array(cols[i], dtype='datetime64[ns]')
                arr = pa.Array.from_pandas(np_arr, type=arrow_type)
            else:
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
