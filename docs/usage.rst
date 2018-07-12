=====
Usage
=====

------------
Installation
------------
Assuming you have Python and pip installed, you can simply run the following::
    pip install spectrify

-----------
Basic Usage
-----------
Spectrify can be used as a command-line tool to “copy” an entire table from Redshift to Redshift Spectrum.

    spectrify --host=example-url.redshift.aws.com
    --user=myuser
    --db=mydb
    transform my_table
    's3://example-bucket/my_table'

This will perform the following:

- Unload the table to s3://example-bucket/my_table/csv/
- Properly encode the data into Parquet files in s3://example-bucket/my_table/spectrum/
- Create a table of the same name in the spectrum schema in your Redshift cluster

Note that the invocation above creates a single partition, and uses a max CSV file size of 1GB,
which for our data translates into parquet files of around 800MB.  By default, the Parquet files
are compressed using gzip compression.

---------------------
Customizing Spectrify
---------------------
Spectrify can also be used as a code library to create your own Spectrum data pipeline.  To
give a real-world use case: say you would like to export your data weekly, with each week’s
data residing in a separate partition.  You would need to modify Spectrify’s default behavior
in the following ways:

- In Redshift, unload only the records from the previous week
- In S3, store CSVs for each week into a separate folder
- In S3, store each week’s Parquet files in a separate folder
- In Redshift Spectrum, Add a new partition instead of creating a new table

A full code listing for this example can be found `in the repository
<https://github.com/hellonarrativ/spectrify/blob/master/examples/weekly_partitions.py>`_.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Unloading a Subset of a Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In Spectrify, the class ``RedshiftDataExporter`` responsible for unloading records.  By modifying
the SQL statement used to perform the unload, you can instruct Spectrify to export a subset
of a table. In the scenario above, it might look like this::

    from spectrify.export import RedshiftDataExporter

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

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Customizing S3 Data Locations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to perform operations involving S3, Spectrify requires you to pass in a ``S3Config``
object.  At its simplest, an S3Config simply points to directories where the CSV and
Parquet/Spectrum files should be stored.  You can specify these directories easily by
creating a SimpleS3Config as shown below::

    from spectrify.utils.s3 import SimpleS3Config

    csv_path = 's3://my-temp-bucket/my-table'
    spectrum_path = 's3://my-spectrum-bucket/my-table'
    s3_config = SimpleS3Config(csv_path, spectrum_path)

^^^^^^^^^^^^^^^^^^^^^^^
Creating New Partitions
^^^^^^^^^^^^^^^^^^^^^^^
In Spectrify, the class ``SpectrumTableCreator`` is responsible for creating Redshift Spectrum
external tables.  If you already have a table created, you can modify the class to create a
new partition instead.  It might look like this::

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

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Incorporating Customizations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The classes above describe how to modify individual aspects of Spectrify.  The following class
demonstrates how you can bring these pieces together with the rest of Spectrify’s functionality.
The class ``TableTransformer`` encompasses all pieces of the conversion from Redshift to
Redshift Spectrum.  Here we override the export and table creation steps with our own
partition strategy::

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
