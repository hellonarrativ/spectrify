from __future__ import absolute_import, division, print_function, unicode_literals
import boto3
import click

UNLOAD_QUERY = """
UNLOAD ('select * from {}')
to %(s3_path)s
CREDENTIALS %(credentials)s
ESCAPE MANIFEST GZIP ALLOWOVERWRITE
MAXFILESIZE 1 gb;
"""


def export_to_csv(engine, table_name, s3_csv_path):
    session = boto3.Session()
    credentials = session.get_credentials()
    creds_str = 'aws_access_key_id={};aws_secret_access_key={}'.format(
        credentials.access_key,
        credentials.secret_key,
    )

    formatted_query = UNLOAD_QUERY.format(table_name)
    with engine.connect() as cursor:
        click.echo('Exporting table to CSV...')
        cursor.execute(formatted_query, {
            's3_path': s3_csv_path,
            'credentials': creds_str,
        })
        click.echo('Done.')

    manifest_path = s3_csv_path + 'manifest'
    return manifest_path
