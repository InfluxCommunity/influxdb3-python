"""Commons function for Sync and Async client."""
from __future__ import absolute_import
# TODO should this file be refactored and remove?
try:
    # import dataclasses

    _HAS_DATACLASS = True
except ModuleNotFoundError:
    _HAS_DATACLASS = False
