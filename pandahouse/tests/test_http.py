import pytest

from requests.exceptions import ConnectionError
from pandahouse.http import execute, ClickhouseException


def test_execute(host):
    query = 'DESC system.parts FORMAT CSV;'
    response = execute(query, host=host)
    assert isinstance(response, bytes)


def test_execute_stream(host):
    query = 'DESC system.parts FORMAT CSV;'
    response = execute(query, host=host, stream=True)
    result = response.read()
    assert result


def test_wrong_host():
    query = 'DESC system.parts FORMAT CSV;'
    with pytest.raises(ConnectionError):
        execute(query, host='http://local')


def test_wrong_query(host):
    query = 'SELECT * FROM default.nonexisting'
    with pytest.raises(ClickhouseException):
        execute(query, host=host)


