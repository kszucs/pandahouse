import re
import codecs

import numpy as np
from toolz import itemmap, keymap


MAPPING = {'object': 'String',
           'uint64': 'UInt64',
           'uint32': 'UInt32',
           'uint16': 'UInt16',
           'float64': 'Float64',
           'float32': 'Float32',
           'uint8': 'UInt8',
           'int64': 'Int64',
           'int32': 'Int32',
           'int16': 'Int16',
           'int8': 'Int8',
           'datetime64[D]': 'Date',
           'datetime64[ns]': 'DateTime'}

PD2CH = keymap(np.dtype, MAPPING)
CH2PD = itemmap(reversed, MAPPING)
CH2PD['Null'] = 'object'


SPECIAL_CHARS = {
    '\b': '\\b',
    '\f': '\\f',
    '\r': '\\r',
    '\n': '\\n',
    '\t': '\\t',
    '\0': '\\0',
    '\\': '\\\\',
    "'": "\\'"
}


def escape(value, quote='`'):
    if not isinstance(value, (str, bytes)):
        return value
    value = ''.join(SPECIAL_CHARS.get(c, c) for c in value)
    if quote == '`':
        return '`{}`'.format(value)
    elif quote == '\'':
        return '\'{}\''.format(value)
    else:
        return value


ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)


def _decode_match(match):
    return codecs.decode(match.group(0), 'unicode-escape')


def decode_escape_sequences(s):
    return ESCAPE_SEQUENCE_RE.sub(_decode_match, s)
