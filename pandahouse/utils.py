import re
import codecs


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


def encode_enum(dtype):
    values = ['{} = {}'.format(escape(val, quote="'"), i)
              for i, val in enumerate(dtype.categories)]
    return 'Enum16({})'.format(', '.join(values))


def decode_enum(chtype):
    chtype = decode_escapes(chtype)
    values = re.match('Enum(8|16)\((.*)\)', chtype).group(2)
    values = [re.match('\'(.*)\'\s+=\s+(\d+)', v) 
              for v in re.split(',\s*', values)]
    return {m.group(1): int(m.group(2)) for m in values}
