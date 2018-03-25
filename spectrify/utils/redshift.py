import sqlalchemy as sa


class ConnectionParameters(object):
    def __init__(self, **kwargs):
        required = ['host', 'port', 'user', 'password', 'db']
        for attr in required:
            val = kwargs.get(attr)
            if not val:
                raise ValueError('{} is required'.format(attr))
            setattr(self, attr, val)

    def validate(self):
        pass


def get_sa_engine(ctx):
    parms = ctx.obj
    url = 'redshift+psycopg2://{user}:{passwd}@{host}:{port}/{database}'.format(
        user=parms.user,
        passwd=parms.password,
        host=parms.host,
        port=parms.port,
        database=parms.db,
    )

    return sa.create_engine(url, connect_args={'sslmode': 'prefer'})
