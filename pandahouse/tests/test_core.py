import pytest

import random
import datetime
import numpy as np
import pandas as pd


from pandas.util.testing import assert_frame_equal

from pandahouse.http import execute
from pandahouse.core import to_clickhouse, read_clickhouse


@pytest.fixture(scope='module')
def df():
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 3)),
                      columns=list('ABC'))
    df['D'] = '2017-05-03'
    return df.set_index('A')


@pytest.yield_fixture(scope='module', autouse=True)
def database(connection):
    # TODO: test every dtyped from .utils
    create = 'CREATE DATABASE IF NOT EXISTS test'
    drop = 'DROP DATABASE IF EXISTS test'
    try:
        yield execute(create, connection=connection)
    finally:
        execute(drop, connection=connection)


@pytest.yield_fixture(scope='module', autouse=True)
def table(connection, database):
    # TODO: test every dtyped from .utils
    create = '''
        CREATE TABLE IF NOT EXISTS test.decimals (
            A UInt64, B Int32, C UInt16, D Date
        ) ENGINE = MergeTree(D, (A), 8192)
    '''
    drop = 'DROP TABLE IF EXISTS test.decimals'
    try:
        yield execute(create, connection=connection)
    finally:
        execute(drop, connection=connection)


def test_insert(df, connection):
    affected_rows = to_clickhouse(df, table='decimals', connection=connection)
    assert affected_rows == 100

    df_ = read_clickhouse('SELECT * FROM {db}.decimals', index_col='A',
                          connection=connection)
    assert df.shape == df_.shape
    assert df.columns.tolist() == df_.columns.tolist()


def test_query(df, connection):
    df_ = read_clickhouse('SELECT B, C FROM {db}.decimals', index_col='B',
                          connection=connection)
    assert df_.shape == (100, 1)


def test_read_special_values(connection):
    ''' Tests empty string values, and String values with special UTF-8 chars.
    '''
    random_id = random.getrandbits(128)
    date = datetime.date(2017, 1, 1)
    create_table = '''CREATE TABLE IF NOT EXISTS {db}.testxyz (
        id String,
        sss String,
        date Date
    ) ENGINE = MergeTree(date, (id), 8192);
    '''.format(db=connection['database'])
    execute(create_table, connection=connection)
    df = pd.DataFrame([[str(random_id), 'joe\\\t\\t\t\u00A0jane\njack',
                        pd.to_datetime(date)],
                       [str(random_id + 1), 'james\u2620johnny',
                        pd.to_datetime(date)],
                       [str(random_id + 2), '', pd.to_datetime(date)]],
                      columns=['id', 'sss', 'date'])
    to_clickhouse(df, 'testxyz', index=False, connection=connection)
    read_query = '''
        SELECT * FROM {{db}}.testxyz
            WHERE id='{}' OR id='{}' OR id='{}';
    '''.format(*list(range(random_id, random_id + 3)))
    read_df = read_clickhouse(read_query, connection=connection)

    assert_frame_equal(df.sort_values('id').set_index('id'),
                       read_df.sort_values('id').set_index('id'))


def test_write_read_column_order(connection):
    random_id = random.getrandbits(128)
    date = datetime.date(2017, 1, 1)
    create_table = '''CREATE TABLE IF NOT EXISTS {db}.testxyz2 (
        id String,
        joe UInt64,
        sss String,
        date Date,
        jessy Int32
    ) ENGINE = MergeTree(date, (id), 8192);
    '''.format(db=connection['database'])
    execute(create_table, connection=connection)
    df = pd.DataFrame([[str(random_id),
                        'joe\\\t\\t\t\u00A0jane\njack',
                        15,
                        pd.to_datetime(date),
                        125]],
                      columns=['id', 'sss', 'joe', 'date', 'jessy']
                      )
    df['joe'] = df['joe'].astype(np.uint64)
    df['jessy'] = df['jessy'].astype(np.int32)
    to_clickhouse(df, 'testxyz2', index=False, connection=connection)
    read_query = "SELECT * FROM {{db}}.testxyz2 WHERE id='{}';".format(
        random_id)
    read_df = read_clickhouse(read_query, connection=connection)

    assert_frame_equal(df.reindex_axis(sorted(df.columns), axis=1)
                       .sort_values('id') .set_index('id'),
                       read_df.reindex_axis(sorted(df.columns), axis=1)
                       .sort_values('id') .set_index('id'))
