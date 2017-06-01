import csv

import pandas as pd
from toolz import keymap, valmap

from .http import execute
from .utils import PD2CH, CH2PD, escape


def normalize(df, index=True):
    if index:
        df = df.reset_index()

    for col in df.select_dtypes([bool]):
        df[col] = df[col].astype('uint8')

    dtypes = valmap(PD2CH.get, dict(df.dtypes))
    if None in dtypes.values():
        raise ValueError('Unknown type mapping in dtypes: {}'.format(dtypes))

    return dtypes, df


def to_csv(df):
    return df.to_csv(header=False, index=False, quoting=csv.QUOTE_NONNUMERIC,
                     encoding='utf-8')


def partition(df, chunksize=1000):
    nrows = df.shape[0]
    nchunks = int(nrows / chunksize) + 1
    for i in range(nchunks):
        start_i = i * chunksize
        end_i = min((i + 1) * chunksize, nrows)
        if start_i >= end_i:
            break

        chunk = df.iloc[start_i:end_i]
        yield chunk


def selection(query, tables=None, index=True):
    query = query.strip().strip(';')
    query = '{} FORMAT TSVWithNamesAndTypes'.format(query)

    external = {}
    tables = tables or {}
    for name, df in tables.items():
        dtypes, df = normalize(df, index=index)
        # dtypes = keymap(escape, dtypes)
        data = to_csv(df)
        structure = ', '.join(map(' '.join, dtypes.items()))
        external[name] = (structure, data)

    return query, external


def insertion(df, table, index=True):
    insert = 'INSERT INTO {db}.{table} ({columns}) FORMAT CSV'
    dtypes, df = normalize(df, index=index)

    columns = ', '.join(map(escape, dtypes.keys()))
    query = insert.format(db='{db}', columns=columns, table=escape(table))

    return query, df


def parse(lines, **kwargs):
    names = lines.readline().decode('utf-8').strip().split('\t')
    types = lines.readline().decode('utf-8').strip().split('\t')
    dtypes, dtimes = {}, []
    for name, chtype in zip(names, types):
        dtype = CH2PD[chtype]
        if dtype.startswith('datetime'):
            dtimes.append(name)
        else:
            dtypes[name] = dtype

    return pd.read_table(lines, header=None, names=names,
                         dtype=dtypes, parse_dates=dtimes,
                         **kwargs)


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
    return parse(lines, **kwargs)


def to_clickhouse(df, table, index=True, chunksize=1000, connection=None):
    query, df = insertion(df, table, index=index)
    for chunk in partition(df, chunksize=chunksize):
        execute(query, data=to_csv(chunk), connection=connection)

    return df.shape[0]
