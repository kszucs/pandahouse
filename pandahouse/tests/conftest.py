import os
import pytest


@pytest.fixture(scope='session')
def connection():
    return {'host': os.environ.get('CLICKHOUSE_HOST', 'http://localhost:8123')}
