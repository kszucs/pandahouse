import csv
import sys
import numpy as np
import pandas as pd
from functools import partial
from collections import OrderedDict
from toolz import itemmap, keymap, valmap

from .utils import escape, decode_escapes
from .utils import encode_enum, decode_enum


PY3 = sys.version_info[0] == 3

MAPPING = {'object': 'String',
           'uint64': 'UInt64',
           'uint32': 'UInt32',
           'uint16': 'UInt16',
           'uint8': 'UInt8',
           'float64': 'Float64',
           'float32': 'Float32',
           'int64': 'Int64',
           'int32': 'Int32',
           'int16': 'Int16',
           'int8': 'Int8',
           'datetime64[D]': 'Date',
           'datetime64[ns]': 'DateTime'}

PD2CH = keymap(np.dtype, MAPPING)
CH2PD = itemmap(reversed, MAPPING)
CH2PD['Null'] = 'object'

NULLABLE_COLS = ['UInt64', 'UInt32', 'UInt16', 'UInt8', 'Float64', 'Float32',
                 'Int64', 'Int32', 'Int16', 'Int8']

#for col in NULLABLE_COLS:
#    CH2PD['Nullable({})'.format(col)] = CH2PD[col]



def normalize(df, index=True):
    if index:
        df = df.reset_index()

    for col in df.select_dtypes([bool]):
        df[col] = df[col].astype('uint8')

    # for col in df.select_dtypes(['category']):
    #     df[col] = df[col].astype(object)

    types = OrderedDict()
    for name, dtype in df.dtypes.items():
        if dtype.name == 'category':
            types[name] = encode_enum(dtype)
        else:
            try:
                types[name] = PD2CH[dtype]
            except KeyError:
                msg = 'Unknown type mapping for dtype: `{}`'
                raise ValueError(msg.format(dtype))

    return types, df


def to_csv(df):
    data = df.to_csv(header=False, index=False, encoding='utf-8',
                     quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    if PY3:
        return data.encode('utf-8')
    else:
        return data


def to_dataframe(lines, **kwargs):
    names = lines.readline().decode('utf-8').strip().split('\t')
    types = lines.readline().decode('utf-8').strip().split('\t')

    dtypes, parse_dates, converters, categoricals = {}, [], {}, {}
    for name, chtype in zip(names, types):
        if chtype.startswith('Enum'):
            dtype = 'object'
            categoricals[name] = decode_enum(chtype)
        else:
            dtype = CH2PD[chtype]

        if dtype == 'object':
            converters[name] = decode_escapes
        elif dtype.startswith('datetime'):
            parse_dates.append(name)
        else:
            dtypes[name] = dtype

    df = pd.read_table(lines, header=None, names=names, dtype=dtypes,
                       parse_dates=parse_dates, converters=converters,
                       na_values=set(), keep_default_na=False, **kwargs)
    
    for name, values in categoricals.items():
        df[name] = pd.Categorical(df[name], categories=values)

    return df


def partition(df, chunksize=1000):
    nrows = df.shape[0]
    nchunks = int(nrows / chunksize) + 1
    for i in range(nchunks):
        start_i = i * chunksize
        end_i = min((i + 1) * chunksize, nrows)
        if start_i >= end_i:
            break

        chunk = df.iloc[start_i:end_i]
        yield chunk

