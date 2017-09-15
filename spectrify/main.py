# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
"""Console script for spectrify."""

import click

from spectrify.create import create_external_table
from spectrify.transform import transform_table
from spectrify.utils.redshift import ConnectionParameters, get_sa_engine
from spectrify.utils.schema import get_table_schema


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
@click.pass_context
def transform(ctx, table, s3_path, dest_schema, dest_table):
    dest_table = dest_table or table
    engine = get_sa_engine(ctx)
    transform_table(engine, table, s3_path, dest_schema, dest_table)


@cli.command()
@click.pass_context
def export():
    click.echo('Export to CSV')


@cli.command()
@click.pass_context
def convert():
    click.echo('Convert to parquet')


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
    click.echo('Add Spectrum partition')
