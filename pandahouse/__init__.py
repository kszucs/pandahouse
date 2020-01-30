from .core import read_clickhouse, to_clickhouse
from .http import execute
from .utils import escape

from functools import lru_cache

from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution


@lru_cache()
def get_version(dist: str, default: str = "local"):
    try:
        return get_distribution(dist).version
    except DistributionNotFound:
        return default


__version__ = get_version("pandahouse")
