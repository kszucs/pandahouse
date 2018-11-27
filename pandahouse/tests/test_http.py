import pytest

from requests.exceptions import RequestException, ConnectionError
from pandahouse.http import execute, ClickhouseException


def test_execute(host):
    query = 'DESC system.parts FORMAT CSV;'
    response = execute(query, connection=dict(host=host))
    assert isinstance(response, bytes)


def test_execute_stream(host):
    query = 'DESC system.parts FORMAT CSV;'
    response = execute(query, stream=True, connection=dict(host=host))
    result = response.read()
    assert result


def test_wrong_host():
    query = 'DESC system.parts FORMAT CSV;'
    with pytest.raises(ConnectionError):
        execute(query, connection=dict(host='http://local'))


def test_wrong_query(host):
    query = 'SELECT * FROM default.nonexisting'
    with pytest.raises((ClickhouseException, RequestException)):
        execute(query, connection=dict(host=host))
