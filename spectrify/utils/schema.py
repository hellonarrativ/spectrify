from __future__ import absolute_import, division, print_function, unicode_literals

import click
import sqlalchemy as sa


def get_table_schema(engine, table_name):
    meta = sa.MetaData(engine)
    # TODO: Verify all columns are supported
    return sa.Table(table_name, meta, autoload=True,
                      postgresql_ignore_search_path=True)
