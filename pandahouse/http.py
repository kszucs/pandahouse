import requests

from requests.exceptions import RequestException
from toolz import valfilter


class ClickhouseException(Exception):
    pass


def execute(query, host, data=None, external={}, user=None,
            password=None, stream=False):
    params = {'query': query, 'user': user, 'password': password}
    params = valfilter(lambda x: x, params)

    files = {}
    for name, (structure, format, serialized) in external.items():
        params['{}_format'.format(name)] = format
        params['{}_structure'.format(name)] = structure
        files[name] = serialized

    if data is not None:
        data = data.encode('utf-8')
    response = requests.post(host, params=params, data=data, stream=stream)

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
