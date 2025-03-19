"""Package for tests/util module."""
import asyncio
import inspect
import sys
import traceback

import pandas


def asyncio_run(async_func):
    def wrapper(*args, **kwargs):
        try:
            return asyncio.run(async_func(*args, **kwargs))
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            raise e

    wrapper.__signature__ = inspect.signature(async_func)
    return wrapper


def lp_to_py_object(lp: str):
    """
    Result format matches the format of objects returned in pyarrow.Table.to_pylist.

    For verifying test data returned from queries.

    :param lp: a lineprotocol formatted string
    :return: a list object
    """
    result = {}
    groups = lp.split(' ')

    tags = groups[0].split(',')
    tags.remove(tags[0])
    for tag in tags:
        t_set = tag.split('=')
        result[t_set[0]] = t_set[1]

    fields = groups[1].split(',')
    for field in fields:
        f_set = field.split('=')
        lastchar = f_set[1][len(f_set[1]) - 1]
        match lastchar:
            case 'i': # integer
                result[f_set[0]] = int(f_set[1].replace('i',''))
            case 'u': # unsigned integer
                result[f_set[0]] = int(f_set[1].replace('u',''))
            case '"': # string
                result[f_set[0]] = f_set[1].replace('"',"")
            case 'e' | 'E' | 't' | 'T' | 'f' | 'F':
                if f_set[1][0].lower() == 't':
                    result[f_set[0]] = True
                else:
                    result[f_set[0]] = False
            case _: # assume float
                result[f_set[0]] = float(f_set[1])

    result['time'] = pandas.Timestamp(int(groups[2]))
    return result
