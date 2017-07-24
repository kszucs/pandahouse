import pytest

import datetime
import numpy as np
import pandas as pd

from pandahouse.http import execute
from pandahouse.core import to_clickhouse, read_clickhouse
from pandahouse.utils import decode_escapes

from pandas.util.testing import assert_frame_equal


@pytest.fixture(scope='module')
def df():
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 3)),
                      columns=list('ABC'))
    df['D'] = '2017-05-03'
    return df.set_index('A')


@pytest.yield_fixture(scope='module')
def database(connection):
    create = 'CREATE DATABASE IF NOT EXISTS {db}'
    drop = 'DROP DATABASE IF EXISTS {db}'
    try:
        yield execute(create, connection=connection)
    finally:
        execute(drop, connection=connection)


@pytest.yield_fixture
def decimals(connection, database):
    create = '''
        CREATE TABLE IF NOT EXISTS {db}.decimals (
            A UInt64, B Int32, C UInt16, D Date
        ) ENGINE = MergeTree(D, (A), 8192)
    '''
    drop = 'DROP TABLE IF EXISTS {db}.decimals'
    try:
        yield execute(create, connection=connection)
    finally:
        execute(drop, connection=connection)


@pytest.yield_fixture
def xyz(connection, database):
    create = '''
        CREATE TABLE IF NOT EXISTS {db}.xyz (
            id Int64,
            sss String,
            date Date
        ) ENGINE = MergeTree(date, (id), 8192);
    '''
    drop = 'DROP TABLE IF EXISTS {db}.xyz'
    try:
        yield execute(create, connection=connection)
    finally:
        execute(drop, connection=connection)


@pytest.yield_fixture
def xyz2(connection, database):
    create = '''
        CREATE TABLE IF NOT EXISTS {db}.xyz2 (
            id Int64,
            joe UInt64,
            sss String,
            date Date,
            jessy Int32
        ) ENGINE = MergeTree(date, (id), 8192);
    '''
    drop = 'DROP TABLE IF EXISTS {db}.xyz2'
    try:
        yield execute(create, connection=connection)
    finally:
        execute(drop, connection=connection)


def test_insert(df, decimals, connection):
    affected_rows = to_clickhouse(df, table='decimals', connection=connection)
    assert affected_rows == 100

    df_ = read_clickhouse('SELECT * FROM {db}.decimals', index_col='A',
                          connection=connection)
    assert df.shape == df_.shape
    assert df.columns.tolist() == df_.columns.tolist()


def test_query(df, decimals, connection):
    affected_rows = to_clickhouse(df, table='decimals', connection=connection)
    assert affected_rows == 100

    df_ = read_clickhouse('SELECT B, C FROM {db}.decimals', index_col='B',
                          connection=connection)
    assert df_.shape == (100, 1)


def test_read_special_strings(connection, xyz):
    """Tests empty and special UTF-8 string values"""

    date = pd.to_datetime(datetime.date(2017, 1, 1))
    data = [[1, 'joe\\\t\\t\t\u00A0jane\njack', date],
            [2, 'james\u2620johnny', date],
            [3, '', date]]
    expected = pd.DataFrame(data, columns=['id', 'sss', 'date'])
    to_clickhouse(expected, 'xyz', index=False, connection=connection)

    query = 'SELECT * FROM {db}.xyz WHERE id IN (1, 2, 3)'
    df = read_clickhouse(query, connection=connection)

    assert_frame_equal(df, expected)


def test_write_read_column_order(connection, xyz2):
    date = pd.to_datetime(datetime.date(2017, 1, 1))
    data = [[1, 'joe\\\t\\t\t\u00A0jane\njack', 15, date, 125]]
    columns = ['id', 'sss', 'joe', 'date', 'jessy']

    expected = pd.DataFrame(data, columns=columns)
    expected['joe'] = expected['joe'].astype(np.uint64)
    expected['jessy'] = expected['jessy'].astype(np.int32)
    to_clickhouse(expected, 'xyz2', index=False, connection=connection)

    query = 'SELECT * FROM {db}.xyz2 WHERE id=1;'
    df = read_clickhouse(query, connection=connection)

    assert_frame_equal(df.reindex_axis(sorted(df.columns), axis=1),
                       expected.reindex_axis(sorted(df.columns), axis=1))
