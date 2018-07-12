"""
This file demonstrates how you can customize/extend the TableTransformer class.
Running this script will:
 - Export previous week's data
 - Convert previous week's data to Parquet
 - Add a partition containing previous week's data to an existing Spectrum table, keyed on date
"""
from datetime import datetime, timedelta
from getpass import getpass
from time import monotonic

import sqlalchemy as sa
from spectrify.utils.s3 import SimpleS3Config
from spectrify.transform import TableTransformer
from spectrify.export import RedshiftDataExporter
from spectrify.create import SpectrumTableCreator

csv_path_template = 's3://my-bucket/my-table/csv/{start.year}/{start.month:02d}/{start.day:02d}'
spectrum_path_template = 's3://my-bucket/my-table/spectrum/partition_key={start}'


def get_redshift_engine():
    """Helper function for getting a SqlAlechemy engine for Redshift.
    Update with your own configuration settings.
    """
    user = input('Redshift User: ')
    password = getpass('Password: ')
    url = 'redshift+psycopg2://{user}:{passwd}@{host}:{port}/{database}'.format(
        user=user,
        passwd=password,
        host='my-redshift-cluster.example.site',
        port=5439,
        database='mydb',
    )
    return sa.create_engine(url, connect_args={'sslmode': 'prefer'})


class WeeklyDataTransformer(TableTransformer):
    """The TableTransformer does 3 things:
      - Export Redshift data to CSV
      - Convert CSV to Parquet
      - Create a Spectrum table from Parquet files

    This subclass overrides the default behavior in the following ways:
      - Exports only last week of data (via WeeklyDataExporter)
      - Adds a partition instead of creating new table (via SpectrumPartitionCreator)
    """
    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        super().__init__(*args, **kwargs)

    def export_redshift_table(self):
        """Overrides the export behavior to only export the last week's data"""
        exporter = WeeklyDataExporter(
            self.engine,
            self.s3_config,
            start_date=self.start_date,
            end_date=self.end_date
        )
        exporter.export_to_csv(self.table_name)

    def create_spectrum_table(self):
        """Overrides create behavior to add a new partition to an existing table"""
        creator = SpectrumPartitionCreator(
            self.engine,
            self.spectrum_schema,
            self.spectrum_name,
            self.sa_table,
            self.s3_config,
            start_date=self.start_date,
            end_date=self.end_date
        )
        creator.create()


class WeeklyDataExporter(RedshiftDataExporter):
    """This class overrides the export query in the following ways:
       - Exports only records with timestamp_col between start_date and end_date
       - Features a smaller MAXFILESIZE (256MB)
    """
    UNLOAD_QUERY = """
    UNLOAD ($$
      SELECT * 
      FROM {table_name}
      WHERE timestamp_col >= '{start_date}' AND timestamp_col < '{end_date}'
    $$)
    TO %(s3_path)s
    CREDENTIALS %(credentials)s
    ESCAPE MANIFEST GZIP ALLOWOVERWRITE
    MAXFILESIZE 256 MB;
    """

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        super().__init__(*args, **kwargs)

    def get_query(self, table_name):
        return self.UNLOAD_QUERY.format(
            table_name=table_name,
            start_date=self.start_date,
            end_date=self.end_date,
        )


class SpectrumPartitionCreator(SpectrumTableCreator):
    """Instead of issuing a CREATE TABLE statement, this subclass creates a
    new partition.
    """
    create_query = """
    ALTER TABLE {spectrum_schema}.{dest_table}
    ADD partition(partition_key='{start_date}')
    LOCATION '{partition_path}';
    """

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        super().__init__(*args, **kwargs)

    def format_query(self):
        partition_path = self.s3_config.spectrum_dir
        return self.create_query.format(
            spectrum_schema=self.schema_name,
            dest_table=self.table_name,
            start_date=self.start_date,
            partition_path=partition_path,
        )


def spectrify_last_week():
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=7)

    # Replace with your table names (or pass in as parameters)
    source_table = 'my_table'
    dest_table = 'my_table'
    spectrum_schema = 'spectrum'

    sa_engine = get_redshift_engine()

    # Construct a S3Config object with the source CSV folder and
    # destination Spectrum/Parquet folder on S3.
    csv_path = csv_path_template.format(start=start_date)
    spectrum_path = spectrum_path_template.format(start=start_date)
    s3_config = SimpleS3Config(csv_path, spectrum_path)

    transformer = WeeklyDataTransformer(
        sa_engine, source_table, s3_config, spectrum_schema, dest_table,
        start_date=start_date, end_date=end_date)
    transformer.transform()


def main():
    start = monotonic()
    spectrify_last_week()
    end = monotonic()
    print('Spectrify operation completed in {} seconds'.format(end - start))


if __name__ == '__main__':
    main()
