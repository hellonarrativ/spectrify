from __future__ import absolute_import, division, print_function, unicode_literals
from future.standard_library import install_aliases
install_aliases()  # noqa

import boto3
import click


class RedshiftDataExporter:
    UNLOAD_QUERY = """
    UNLOAD ('select * from {}')
    to %(s3_path)s
    CREDENTIALS %(credentials)s
    ESCAPE MANIFEST GZIP ALLOWOVERWRITE
    REGION 'us-east-1'
    MAXFILESIZE 256 mb;
    """

    def __init__(self, sa_engine, s3_config):
        self.sa_engine = sa_engine
        self.s3_config = s3_config

    def export_to_csv(self, table_name):
        s3_path = self.s3_config.get_csv_dir()
        creds_str = self.get_credentials()
        query = self.get_query(table_name)

        with self.sa_engine.connect() as cursor:
            click.echo('Exporting table to CSV...')
            cursor.execute(query, {
                's3_path': s3_path,
                'credentials': creds_str,
            })
            click.echo('Done.')

    def get_query(self, table_name):
        return self.UNLOAD_QUERY.format(table_name)

    def get_credentials(self):
        session = boto3.Session()
        credentials = session.get_credentials()
        return 'aws_access_key_id={};aws_secret_access_key={}'.format(
            credentials.access_key,
            credentials.secret_key,
        )
