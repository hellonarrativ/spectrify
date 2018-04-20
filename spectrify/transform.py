from __future__ import absolute_import, division, print_function, unicode_literals

from spectrify.convert import ConcurrentManifestConverter
from spectrify.create import SpectrumTableCreator
from spectrify.export import RedshiftDataExporter
from spectrify.utils.schema import SqlAlchemySchemaReader


class TableTransformer:
    def __init__(self, engine, table_name, s3_config, spectrum_schema, spectrum_name):
        self.engine = engine
        self.table_name = table_name
        self.s3_config = s3_config
        self.spectrum_schema = spectrum_schema
        self.spectrum_name = spectrum_name
        self.sa_table = SqlAlchemySchemaReader(engine).get_table_schema(table_name)

    def transform(self):
        self.export_redshift_table()
        self.convert_csv_data()
        self.create_spectrum_table()

    def export_redshift_table(self):
        exporter = RedshiftDataExporter(self.engine, self.s3_config)
        exporter.export_to_csv(self.table_name)

    def convert_csv_data(self):
        converter = ConcurrentManifestConverter(self.sa_table, self.s3_config)
        converter.convert_manifest()

    def create_spectrum_table(self):
        table_creator = SpectrumTableCreator(
            self.engine,
            self.spectrum_schema,
            self.spectrum_name,
            self.sa_table,
            self.s3_config
        )
        table_creator.log_query()
        table_creator.confirm()
        table_creator.create()
