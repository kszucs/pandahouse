import pytest

from requests.exceptions import RequestException, ConnectionError
from pandahouse.http import execute, ClickhouseException


def test_execute(connection):
    query = 'DESC system.parts FORMAT CSV;'
    response = execute(query, connection=connection)
    assert isinstance(response, bytes)


def test_execute_stream(connection):
    query = 'DESC system.parts FORMAT CSV;'
    response = execute(query, stream=True, connection=connection)
    result = response.read()
    assert result


def test_wrong_host():
    query = 'DESC system.parts FORMAT CSV;'
    with pytest.raises(ConnectionError):
        execute(query, connection={'host': 'http://local'})


def test_wrong_query(connection):
    query = 'SELECT * FROM default.nonexisting'
    with pytest.raises((ClickhouseException, RequestException)):
        execute(query, connection=connection)
