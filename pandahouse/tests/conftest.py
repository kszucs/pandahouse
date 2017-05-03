import pytest


@pytest.fixture(scope='session')
def host():
    return 'https://localhost:8123'
