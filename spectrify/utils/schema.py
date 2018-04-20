from __future__ import absolute_import, division, print_function, unicode_literals

import sqlalchemy as sa
from spectrify.utils.parquet import Writer


class SchemaReader:
    def get_table_schema(self, table_name):
        """Returns a SqlAlchemy schema.  Schema provides column names and
        data types for use during format conversions
        """
        raise NotImplementedError('Must be implemented by subclass')


class SqlAlchemySchemaReader(SchemaReader):
    """Map between SqlAlchemy type classes and pyarrow/parquet type generator
    functions."""

    def __init__(self, engine):
        self.engine = engine
        self.metadata = sa.MetaData(self.engine)

    def get_table_schema(self, table_name):
        schema_name = None

        # Handle table name prepended with schema
        parts = table_name.split('.')
        if len(parts) == 2:
            schema_name, table_name = parts

        table = sa.Table(
            table_name,
            self.metadata,
            autoload=True,
            postgresql_ignore_search_path=True,
            schema=schema_name
        )
        for col in table.columns:
            if col.type.__class__ not in self.get_supported_sa_types():
                raise ValueError(
                    'Type {} not currently supported by Spectrify. Open an issue?'.format(
                        col.type.__class__
                    )
                )

        return table

    def get_supported_sa_types(self):
        """Override this if you need to implement your own types"""
        return Writer.supported_sa_types
