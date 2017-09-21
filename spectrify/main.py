# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
"""Console script for spectrify."""

import click

from spectrify.convert import convert_redshift_manifest_to_parquet
from spectrify.create import create_external_table
from spectrify.export import export_to_csv
from spectrify.transform import transform_table
from spectrify.utils.redshift import ConnectionParameters, get_sa_engine
from spectrify.utils.schema import get_table_schema
from spectrify.utils.s3 import paths_from_base_path


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
@click.option('--workers', type=int)
@click.pass_context
def transform(ctx, table, s3_path, dest_schema, dest_table, workers):
    dest_table = dest_table or table
    engine = get_sa_engine(ctx)
    transform_table(engine, table, s3_path, dest_schema, dest_table, workers)


@cli.command()
@click.argument('table')
@click.argument('s3_path')
@click.pass_context
def export(ctx, table, s3_path):
    engine = get_sa_engine(ctx)
    s3_csv_path, s3_csv_manifest, s3_spectrum_path = paths_from_base_path(s3_path)
    export_to_csv(engine, table, s3_csv_path)


@cli.command()
@click.argument('table')
@click.argument('s3_path')
@click.option('--workers', type=int)
@click.pass_context
def convert(ctx, table, s3_path, workers):
    engine = get_sa_engine(ctx)
    sa_table = get_table_schema(engine, table)
    s3_csv_path, s3_csv_manifest, s3_spectrum_path = paths_from_base_path(s3_path)
    convert_redshift_manifest_to_parquet(s3_csv_manifest, sa_table, s3_spectrum_path, workers=workers)


@cli.command()
@click.argument('s3-path')
@click.argument('source-table')
@click.argument('dest-table')
@click.option('--dest-schema', default='spectrum')
@click.pass_context
def create_table(ctx, s3_path, source_table, dest_table, dest_schema):
    click.echo('Create Spectrum table')
    engine = get_sa_engine(ctx)
    sa_table = get_table_schema(engine, source_table)
    create_external_table(engine, dest_schema, dest_table, sa_table, s3_path)


@cli.command()
@click.pass_context
def add_part():
    click.echo('TODO: Add Spectrum partition')
