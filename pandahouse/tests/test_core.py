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

@pytest.mark.skip
def test_read_empty_string():
    ''' TODO(Rara): Failing. Fix coming soon.
    '''
    random_id1, random_id2 = sorted(random.getrandbits(128) for _ in range(2))
    config = {'host': 'http://localhost:8123', 'database': 'test'}
    date = datetime.date(2017, 1, 1)
    create_table = '''CREATE TABLE IF NOT EXISTS test.testxyz (
        id String,
        sss String,
        date Date
    ) ENGINE = MergeTree(date, (id), 8192);
    '''
    execute(create_table, connection=config)
    df = pd.DataFrame([[str(random_id1), 'joe', pd.to_datetime(date)],
                       [str(random_id2), 's', pd.to_datetime(date)]],
                      columns=['id', 'sss', 'date'])
    to_clickhouse(df, 'testxyz', index=False, connection=config)
    read_query = f'''
        SELECT * FROM test.testxyz
            WHERE id='{random_id1}' OR id='{random_id2}';
    '''
    read_df = read_clickhouse(read_query, connection=config)
    assert_frame_equal(df, read_df)
