# Copyright (c) 2018 Lightricks. All rights reserved.
from unittest import main, TestCase

import sqlalchemy
import textwrap

from spectrify.create import OpenCSVSerdeTableCreator
from spectrify.utils.s3 import SimpleS3Config


class TestOpenCSVSerdeTableCreator(TestCase):
    def test_query(self):
        expected_query = r"""
            create external table schema.table (
                int_col_1 INTEGER
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
                'separatorChar' = '|',
                'quoteChar' = '\"',
                'escapeChar' = '\\'
            )
            stored as textfile
            location 's3://some_bucket/prefix/csv/manifest'
            table properties (
                'compression_type'='gzip'
            );
        """
        s3_config = SimpleS3Config.from_base_path("s3://some_bucket/prefix")
        sa_meta = sqlalchemy.MetaData()
        sa_table = sqlalchemy.Table(
            'unit_test_table',
            sa_meta,
            sqlalchemy.Column('int_col_1', sqlalchemy.INTEGER),
        )
        open_csv_serde_table_creator = OpenCSVSerdeTableCreator(
            engine=None,
            schema_name="schema",
            table_name="table",
            sa_table=sa_table,
            s3_config=s3_config
        )
        self.assertEqual(
            textwrap.dedent(expected_query),
            textwrap.dedent(open_csv_serde_table_creator.query)
        )


if __name__ == "__main__":
    main()
