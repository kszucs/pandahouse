import csv

import pandas as pd
from toolz import keymap, valmap

from .http import execute
from .utils import PD2CH, CH2PD, escape


def prepare(df, index=True):
    if index:
        df = df.reset_index()

    for col in df.select_dtypes([bool]):
        df[col] = df[col].astype('uint8')

    dtypes = valmap(PD2CH.get, dict(df.dtypes))
    if None in dtypes.values():
        raise ValueError('Unknown type mapping in dtypes: {}'.format(dtypes))

    return dtypes, df


def serialize(df):
    return df.to_csv(header=False, index=False, quoting=csv.QUOTE_NONNUMERIC,
                     encoding='utf-8')


def read_clickhouse(query, host, tables={}, database='default',
                    user=None, password=None, index=True, **kwargs):
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
    query = query.format(db=escape(database)).strip(' ;')
    query = '{} FORMAT TSVWithNamesAndTypes'.format(query)

    external = {}
    for name, df in tables.items():
        dtypes, df = prepare(df, index=index)
        dtypes = keymap(escape, dtypes)
        data = serialize(df)
        structure = ', '.join(map(' '.join, dtypes.items()))
        external[name] = (structure, data)

    lines = execute(query, external=external, host=host,
                    user=user, password=password, stream=True)

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


def to_clickhouse(df, table, host, database='default', user=None,
                  password=None, index=True, chunksize=1000):
    insert = 'INSERT INTO {database}.{table} ({columns}) FORMAT CSV'
    dtypes, df = prepare(df, index=index)

    columns = ', '.join(map(escape, dtypes.keys()))
    query = insert.format(columns=columns, table=escape(table),
                          database=escape(database))

    nrows = df.shape[0]
    nchunks = int(nrows / chunksize) + 1
    for i in range(nchunks):
        start_i = i * chunksize
        end_i = min((i + 1) * chunksize, nrows)
        if start_i >= end_i:
            break

        chunk = df.ix[start_i:end_i]
        execute(query, data=serialize(chunk),
                host=host, user=user, password=password)

    return nrows
