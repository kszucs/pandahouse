import pytest


@pytest.fixture(scope='session')
def host():
    return 'http://localhost:8123'
