import os
import pytest


@pytest.fixture(scope='session')
def connection():
    return {'host': os.environ.get('CLICKHOUSE', 'http://localhost:8123'),
            'database': 'test'}
