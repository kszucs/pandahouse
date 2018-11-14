import re
import codecs
import json


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


def decode_escapes(s):
    return ESCAPE_SEQUENCE_RE.sub(_decode_match, s)


def decode_array(clickhouse_array):
    # Double quotes need escaping before parsed as JSON
    clickhouse_array = clickhouse_array.replace('"', '\\"')
    # Any non-escaped single quotes should be replaced with double quotes
    clickhouse_array = re.sub("(?<!\\\\)'", '"', clickhouse_array)
    # Finally any escaped single quotes can be replaced with unescaped ones
    clickhouse_array = clickhouse_array.replace("\\'", "'")
    return json.loads(clickhouse_array)
