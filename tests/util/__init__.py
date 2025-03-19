"""Package for tests/util module."""
import asyncio
import inspect
import sys
import traceback

import pandas


def asyncio_run(async_func):
    """
    Fixture for running tests asynchronously

    Example

    .. sourcecode:: python

    @asyncio_run
    async def test_my_feature(self):
        asyncio.sleep(1)
        print("waking...")
        ...

    :param async_func:
    :return:
    """
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

    def check_bool(token):
        if token.lower()[0] == 't':
            return True
        return False

    parse_field_val = {
        'i': lambda s: int(s.replace('i', '')),
        'u': lambda s: int(s.replace('u', '')),
        '\"': lambda s: s.replace('"', ''),
        'e': lambda s: check_bool(s),
        'E': lambda s: check_bool(s),
        't': lambda s: check_bool(s),
        'T': lambda s: check_bool(s),
        'f': lambda s: check_bool(s),
        'F': lambda s: check_bool(s),
        'd': lambda s: float(s)
    }

    for field in fields:
        f_set = field.split('=')
        last_char = f_set[1][len(f_set[1]) - 1]
        if last_char in '0123456789':
            last_char = 'd'
        if last_char in parse_field_val.keys():
            result[f_set[0]] = parse_field_val[last_char](f_set[1])
        else:
            result[f_set[0]] = None

    result['time'] = pandas.Timestamp(int(groups[2]))
    return result
