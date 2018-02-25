from __future__ import absolute_import, division, print_function, unicode_literals

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP


def _pa_timestamp_ns():
    """Wrapper function around Arrow's timestamp type function, which is the
    only type function that requires an argument...
    """
    return pa.timestamp('ns')


# Map between SqlAlchemy type classes and pyarrow/parquet type generator functions.
sa_type_map = {
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


def get_table_schema(engine, table_name):
    meta = sa.MetaData(engine)

    table = sa.Table(table_name, meta, autoload=True, postgresql_ignore_search_path=True)

    for col in table.columns:
        if col.type.__class__ not in sa_type_map:
            raise ValueError(
                'Type {} not currently supported by Spectrify. Open an issue?'.format(
                    col.type.__class__
                )
            )

    return table
