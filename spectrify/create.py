from __future__ import absolute_import, division, print_function
from future.standard_library import install_aliases
install_aliases()  # noqa

import click
from sqlalchemy.schema import CreateColumn
from sqlalchemy import Column, types
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

type_map = {
    DOUBLE_PRECISION: types.FLOAT,  # Replace postgres-specific with more generic
}


class SpectrumTableCreator:
    create_query = """
    create external table {table_name} (
        {column_list}
    )
    stored as parquet
    location '{s3_location}'
    """

    def __init__(self, engine, schema_name, table_name, sa_table, s3_config):
        self.engine = engine
        self.schema_name = schema_name
        self.table_name = table_name
        self.sa_table = sa_table
        self.s3_config = s3_config
        self.query = self.format_query()

    def log(self, msg):
        """By default, we log to console with click"""
        click.echo(msg)

    def format_query(self):
        cols = list(self.sa_table.columns)

        # If we are converting a table from another schema, include the schema
        # in the table name.
        table_name = self.table_name.replace('.', '_')

        col_descriptors = []
        for col in cols:
            # We only want the column name and type.
            # There are no NOT NULL, DEFAULT, etc. clauses in Spectrum
            # Also, we need to replace some types.
            rs_col_cls = col.type.__class__
            if rs_col_cls in type_map:
                spectrum_col_type = type_map[rs_col_cls]()
            else:
                spectrum_col_type = col.type
            spectrum_col = Column(col.description, spectrum_col_type)

            # OK, now get the actual SQL snippet for the column
            statement = CreateColumn(spectrum_col).compile().statement

            col_descriptors.append(str(statement))

        col_ddl = ',\n    '.join(col_descriptors)

        return self.create_query.format(
            table_name='.'.join([self.schema_name, table_name]),
            column_list=col_ddl,
            s3_location=self.s3_config.get_spectrum_dir(),
        )

    def create(self):
        with self.engine.connect() as cursor:
            cursor.execution_options(isolation_level='AUTOCOMMIT')
            self.log('Creating table...')
            cursor.execute(self.query)
            self.log('Done.')

    def log_query(self):
        self.log('')
        self.log('*** CREATE TABLE SQL ***')
        self.log(self.query)
        self.log('')

    def confirm(self):
        click.confirm('Continue?', abort=True)
