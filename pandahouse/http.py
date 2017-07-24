import requests

from requests.exceptions import RequestException
from toolz import valfilter, merge

from .utils import escape


_default = dict(host='http://localhost:8123', database='default',
                user=None, password=None)


class ClickhouseException(Exception):
    pass


def prepare(query, connection=None, external=None):
    connection = merge(_default, connection or {})
    database = escape(connection['database'])
    query = query.format(db=database)
    params = {'query': query,
              'user': connection['user'],
              'password': connection['password']}
    params = valfilter(lambda x: x, params)

    files = {}
    external = external or {}
    for name, (structure, serialized) in external.items():
        params['{}_format'.format(name)] = 'CSV'
        params['{}_structure'.format(name)] = structure
        files[name] = serialized

    host = connection['host']

    return host, params, files


def execute(query, connection=None, data=None, external=None, stream=False):
    host, params, files = prepare(query, connection, external=external)

    response = requests.post(host, params=params, data=data,
                             stream=stream, files=files)

    try:
        response.raise_for_status()
    except RequestException as e:
        if response.content:
            raise ClickhouseException(response.content)
        else:
            raise e

    if stream:
        return response.raw
    else:
        return response.content
