import click
from sqlalchemy.schema import CreateColumn
from sqlalchemy import Column, types
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

create_query = """
create external table {table_name} (
    {column_list}
)
stored as parquet
location '{s3_location}'
"""

type_map = {
    DOUBLE_PRECISION: types.FLOAT,  # Replace postgres-specific with more generic
}


def create_external_table(engine, schema_name, table_name, sa_table, s3_path):
    cols = list(sa_table.columns)

    col_descriptors = []
    for col in cols:
        # We only want the column name and type.
        # There are no NOT NULL, DEFAULT, etc. clauses in Spectrum
        # Also, we need to replace some types.
        rs_col_cls = col.type.__class__
        if rs_col_cls in type_map:
            spectrum_col_type = type_map[rs_col_cls]()
        else:
            spectrum_col_type = col.type
        spectrum_col = Column(col.description, spectrum_col_type)

        # OK, now get the actual SQL snippet for the column
        statement = CreateColumn(spectrum_col).compile().statement

        col_descriptors.append(str(statement))
    col_ddl = ',\n    '.join(col_descriptors)

    formatted_query = create_query.format(
        table_name='.'.join([schema_name, table_name]),
        column_list=col_ddl,
        s3_location=s3_path,
    )

    click.echo('')
    click.echo('*** CREATE TABLE SQL ***')
    click.echo(formatted_query)
    click.echo('')
    click.confirm('Continue?', abort=True)

    with engine.connect() as cursor:
        click.echo('Creating table...')
        cursor.execute(formatted_query)
        click.echo('Done.')
