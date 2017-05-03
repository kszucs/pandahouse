import pytest

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
def database(host):
    # TODO: test every dtyped from .utils
    create = 'CREATE DATABASE IF NOT EXISTS test'
    drop = 'DROP DATABASE IF EXISTS test'
    try:
        yield execute(create, host)
    finally:
        execute(drop, host)


@pytest.yield_fixture(scope='module', autouse=True)
def table(host, database):
    # TODO: test every dtyped from .utils
    create = '''
        CREATE TABLE IF NOT EXISTS test.decimals (
            A UInt64, B Int32, C UInt16, D Date
        ) ENGINE = MergeTree(D, (A), 8192)
    '''
    drop = 'DROP TABLE IF EXISTS test.decimals'
    try:
        yield execute(create, host)
    finally:
        execute(drop, host)


def test_insert(df, host):
    affected_rows = to_clickhouse(df, table='decimals',
                                  database='test', host=host)
    assert affected_rows == 100

    df_ = read_clickhouse('SELECT * FROM {db}.decimals', index_col='A',
                          database='test', host=host)
    assert df.shape == df_.shape
    assert df.columns.tolist() == df_.columns.tolist()


def test_query(df, host):
    df_ = read_clickhouse('SELECT B, C FROM {db}.decimals', index_col='B',
                          database='test', host=host)
    assert df_.shape == (100, 1)

