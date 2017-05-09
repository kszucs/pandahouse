import pytest


@pytest.fixture(scope='session')
def connection():
    return {'host': 'http://localhost:8123',
            'database': 'test'}
