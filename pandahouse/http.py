import requests

from requests.exceptions import RequestException
from toolz import valfilter, merge

from .utils import escape


_default = dict(host='http://localhost:8123', database='default',
                user=None, password=None)


class ClickhouseException(Exception):
    pass


def execute(query, connection={}, data=None, external={}, stream=False):
    connection = merge(_default, connection)
    params = {'query': query.format(db=escape(connection['database'])),
              'user': connection['user'],
              'password': connection['password']}
    params = valfilter(lambda x: x, params)

    files = {}
    for name, (structure, format, serialized) in external.items():
        params['{}_format'.format(name)] = format
        params['{}_structure'.format(name)] = structure
        files[name] = serialized

    if data is not None:
        data = data.encode('utf-8')
    response = requests.post(connection['host'], params=params, data=data,
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
