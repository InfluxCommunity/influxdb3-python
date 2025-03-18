"""Package for tests/util module."""
import asyncio
import inspect
import sys
import traceback


def asyncio_run(async_func):
    def wrapper(*args, **kwargs):
        try:
            return asyncio.run(async_func(*args, **kwargs))
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            raise e

    wrapper.__signature__ = inspect.signature(async_func)
    return wrapper
