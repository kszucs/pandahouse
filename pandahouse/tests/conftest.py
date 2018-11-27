import os
import pytest


@pytest.fixture(scope='session')
def host():
    return os.environ.get('CLICKHOUSE_HOST', 'http://localhost:8123')
