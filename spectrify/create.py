from __future__ import absolute_import, division, print_function
from future.standard_library import install_aliases
import abc
install_aliases()  # noqa

import click
from sqlalchemy.schema import CreateColumn
from sqlalchemy import Column, types
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

type_map = {
    DOUBLE_PRECISION: types.FLOAT,  # Replace postgres-specific with more generic
}


class TableCreator(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, engine, schema_name, table_name, sa_table, s3_config):
        self.engine = engine
        self.schema_name = schema_name
        self.table_name = table_name
        self.sa_table = sa_table
        self.s3_config = s3_config

    @property
    def query(self):
        return self.format_query()

    def log(self, msg):
        """By default, we log to console with click"""
        click.echo(msg)

    def get_table_columns_ddl(self):
        col_descriptors = []
        cols = list(self.sa_table.columns)
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

        return ',\n    '.join(col_descriptors)

    @abc.abstractmethod
    def format_query(self):
        raise NotImplementedError()

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


class SpectrumTableCreator(TableCreator):
    create_query = """
    create external table {table_name} (
        {column_list}
    )
    stored as parquet
    location '{s3_location}'
    """

    def __init__(self, engine, schema_name, table_name, sa_table, s3_config):
        TableCreator.__init__(self, engine, schema_name, table_name, sa_table, s3_config)

    def format_query(self):
        # If we are converting a table from another schema, include the schema
        # in the table name.
        table_name = self.table_name.replace('.', '_')
        return self.create_query.format(
            table_name='.'.join([self.schema_name, table_name]),
            column_list=self.get_table_columns_ddl(),
            s3_location=self.s3_config.get_spectrum_dir(),
        )


class OpenCSVSerdeTableCreator(TableCreator):
    create_query = r"""
    create external table {table_name} (
        {column_list}
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
        'separatorChar' = '{delimiter}',
        'quoteChar' = '\"',
        'escapeChar' = '\\'
    )
    stored as textfile
    location '{s3_location}'
    table properties (
        'compression_type'='{compression}'
    );
    """

    def __init__(
        self,
        engine,
        schema_name,
        table_name,
        sa_table,
        s3_config,
        delimiter="|",
        gzipped=True,
        use_manifest=True
    ):
        TableCreator.__init__(self, engine, schema_name, table_name, sa_table, s3_config)
        self.delimiter = delimiter
        self.gzipped = gzipped
        self.use_manifest = use_manifest

    def format_query(self):
        # If we are converting a table from another schema, include the schema
        # in the table name.
        table_name = self.table_name.replace('.', '_')
        return self.create_query.format(
            table_name='.'.join([self.schema_name, table_name]),
            column_list=self.get_table_columns_ddl(),
            delimiter=self.delimiter,
            s3_location=self._get_s3_location(),
            compression=self._get_compression_level()
        )

    def _get_s3_location(self):
        if self.use_manifest:
            return self.s3_config.get_manifest_path()

        return self.s3_config.get_csv_dir()

    def _get_compression_level(self):
        if self.gzipped:
            return 'gzip'

        return 'none'
