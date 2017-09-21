from __future__ import absolute_import, division, print_function, unicode_literals

import click

from spectrify.convert import convert_redshift_manifest_to_parquet
from spectrify.create import create_external_table
from spectrify.export import export_to_csv
from spectrify.utils.schema import get_table_schema
from spectrify.utils.s3 import paths_from_base_path


def transform_table(engine, table_name, s3_base_path, spectrum_schema, spectrum_name, workers):
    click.echo('transforming!')

    s3_csv_path, s3_manifest, s3_spectrum_path = paths_from_base_path(s3_base_path)

    # Get schema
    sa_table = get_table_schema(engine, table_name)

    # Export
    s3_manifest = export_to_csv(engine, table_name, s3_csv_path)

    # Convert
    convert_redshift_manifest_to_parquet(s3_manifest, sa_table, s3_spectrum_path, workers=workers)

    # Add spectrum table
    create_external_table(engine, spectrum_schema, spectrum_name, sa_table, s3_spectrum_path)
