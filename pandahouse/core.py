from .http import execute
from .utils import escape
from .convert import normalize, partition, to_dataframe, to_csv


def selection(query, tables=None, index=True):
    query = query.strip().strip(';')
    query = '{} FORMAT TSVWithNamesAndTypes'.format(query)

    external = {}
    tables = tables or {}
    for name, df in tables.items():
        dtypes, df = normalize(df, index=index)
        data = to_csv(df)
        structure = ', '.join(map(' '.join, dtypes.items()))
        external[name] = (structure, data)

    return query, external


def insertion(df, table, index=True):
    insert = 'INSERT INTO {db}.{table} ({columns}) FORMAT CSV'
    _, df = normalize(df, index=index)

    columns = ', '.join(map(escape, df.columns))
    query = insert.format(db='{db}', columns=columns, table=escape(table))

    return query, df


def read_clickhouse(query, tables=None, index=True, connection=None, **kwargs):
    """Reads clickhouse query to pandas dataframe

    Parameters
    ----------

    query: str
        Clickhouse sql query, {db} will automatically replaced
        with `database` argument
    host: str
        clickhouse host to connect
    tables: dict of pandas DataFrames
        external table definitions for query processing
    database: str, default 'default'
        clickhouse database
    user: str, default None
        clickhouse user
    password: str, default None
        clickhouse password
    index: bool, default True
        whether to serialize `tables` with index or not

    Additional keyword arguments passed to `pandas.read_table`
    """
    query, external = selection(query, tables=tables, index=index)
    lines = execute(query, external=external, stream=True,
                    connection=connection)
    return to_dataframe(lines, **kwargs)


def to_clickhouse(df, table, index=True, chunksize=1000, connection=None):
    query, df = insertion(df, table, index=index)
    for chunk in partition(df, chunksize=chunksize):
        execute(query, data=to_csv(chunk), connection=connection)

    return df.shape[0]
