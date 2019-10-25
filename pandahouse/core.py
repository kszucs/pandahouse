from .http import execute
from .utils import escape
from .convert import normalize, partition, to_dataframe, to_csv


def selection(query, tables=None, index=True, add_col_names=True):
    if add_col_names:
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


def read_clickhouse(query, tables=None, index=True, connection=None, cert_file=None, as_data_frame=True,
                    add_col_names=True, **kwargs):
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
    cert_file: str, default None
        certificate.pem file path
    as_data_frame: bool, default True
         return object as data frame or raw content of requests
    add_col_names: bool, default True
          whether to add column names with query

    Additional keyword arguments passed to `pandas.read_table`
    """

    query, external = selection(query, tables=tables, index=index, add_col_names=add_col_names)
    if as_data_frame:
        lines = execute(query,
                        external=external,
                        stream=True,
                        connection=connection,
                        cert_file=cert_file)
        return to_dataframe(lines, **kwargs)
    else:
        lines = execute(query,
                        external=external,
                        stream=False,
                        connection=connection,
                        cert_file=cert_file)
        return lines


def to_clickhouse(df, table, index=True, chunksize=1000, connection=None, cert_file=None):
    query, df = insertion(df, table, index=index)
    for chunk in partition(df, chunksize=chunksize):
        execute(query, data=to_csv(chunk), connection=connection, cert_file=cert_file)

    return df.shape[0]
