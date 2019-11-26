# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
"""Console script for spectrify."""

import click

from spectrify.convert import ConcurrentManifestConverter
from spectrify.create import SpectrumTableCreator
from spectrify.export import RedshiftDataExporter
from spectrify.transform import TableTransformer
from spectrify.utils.redshift import ConnectionParameters, get_sa_engine
from spectrify.utils.schema import SqlAlchemySchemaReader
from spectrify.utils.s3 import SimpleS3Config


@click.group()
@click.option('--host', envvar='REDSHIFT_HOST', default='localhost')
@click.option('--port', envvar='REDSHIFT_PORT', default=5439, type=int)
@click.option('--user', envvar='REDSHIFT_USER', default='redshift')
@click.option('--password', envvar='REDSHIFT_PASSWORD', prompt=True, hide_input=True)
@click.option('--db', envvar='REDSHIFT_DB')
@click.pass_context
def cli(ctx, **kwargs):
    """Main entry point for spectrify."""
    parms = ConnectionParameters(**kwargs)
    ctx.obj = parms


@cli.command()
@click.argument('table')
@click.argument('s3_path')
@click.option('--dest-schema', default='spectrum')
@click.option('--dest-table')
@click.option('--s3-region')
@click.pass_context
def transform(ctx, table, s3_path, dest_schema, dest_table, s3_region):
    dest_table = dest_table or table
    engine = get_sa_engine(ctx)
    s3_config = SimpleS3Config.from_base_path(s3_path, region=s3_region)
    transformer = TableTransformer(engine, table, s3_config, dest_schema, dest_table)
    transformer.transform()


@cli.command()
@click.argument('table')
@click.argument('s3_path')
@click.option('--s3-region')
@click.pass_context
def export(ctx, table, s3_path, s3_region):
    engine = get_sa_engine(ctx)
    s3_config = SimpleS3Config.from_base_path(s3_path, region=s3_region)
    RedshiftDataExporter(engine, s3_config).export_to_csv(table)


@cli.command()
@click.argument('table')
@click.argument('s3_path')
@click.pass_context
def convert(ctx, table, s3_path):
    engine = get_sa_engine(ctx)
    sa_table = SqlAlchemySchemaReader(engine).get_table_schema(table)
    s3_config = SimpleS3Config.from_base_path(s3_path)

    converter = ConcurrentManifestConverter(sa_table, s3_config)
    converter.convert_manifest()


@cli.command()
@click.argument('s3-path')
@click.argument('source-table')
@click.argument('dest-table')
@click.option('--dest-schema', default='spectrum')
@click.pass_context
def create_table(ctx, s3_path, source_table, dest_table, dest_schema):
    click.echo('Create Spectrum table')
    engine = get_sa_engine(ctx)
    sa_table = SqlAlchemySchemaReader(engine).get_table_schema(source_table)
    s3_config = SimpleS3Config.from_base_path(s3_path)

    table_creator = SpectrumTableCreator(
        engine,
        dest_schema,
        dest_table,
        sa_table,
        s3_config
    )
    table_creator.log_query()
    table_creator.confirm()
    table_creator.create()


@cli.command()
@click.pass_context
def add_part():
    click.echo('TODO: Add Spectrum partition')
