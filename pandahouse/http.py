import requests
import ssl

from requests.adapters import HTTPAdapter, DEFAULT_POOLSIZE, DEFAULT_POOLSIZE, DEFAULT_RETRIES, DEFAULT_POOLBLOCK
from requests.exceptions import RequestException
from toolz import valfilter, merge

from .utils import escape

_default = dict(host='http://localhost:8123', database='default',
                user=None, password=None)


class ClickhouseException(Exception):
    pass


class SSLAdapter(HTTPAdapter):
    def __init__(self, cert_file,
                 pool_connections=DEFAULT_POOLSIZE,
                 pool_maxsize=DEFAULT_POOLSIZE,
                 max_retries=DEFAULT_RETRIES,
                 pool_block=DEFAULT_POOLBLOCK):
        self.cert_file = cert_file
        super(SSLAdapter, self).__init__(pool_connections, pool_maxsize, max_retries, pool_block)

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.protocol = "SSL"
        ctx.load_verify_locations(self.cert_file)
        kwargs['ssl_context'] = ctx
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)


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


def execute(query, connection=None, data=None, external=None, stream=False, cert_file=None):
    host, params, files = prepare(query, connection, external=external)

    # for insecure ssl without certificate.pem file
    if cert_file == None:
        response = requests.post(host, params=params, verify=False, data=data,
                                 stream=stream, files=files)

    # for secure ssl with certificate.pem file
    else:
        session = requests.Session()
        session.mount(host, SSLAdapter(cert_file))

        response = session.post(host, params=params, data=data, stream=stream, files=files)

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
