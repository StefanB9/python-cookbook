"""Caching decorator for functions returning pd.DataFrame objects"""
import datetime as dt
import os
from typing import Any

from collections import Callable
from functools import wraps
from logging import getLogger

import pandas as pd

log = getLogger('__name__')


def df_cache(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
    """Caching decorator for functions returning pd.DataFrame"""
    cache_dir = 'cache/'

    @wraps(func)
    def inner_func(*args: Any, **kwargs: Any):
        if not args and not kwargs:
            kwargs['runtime'] = dt.date.today()
        file_name = (
            cache_dir
            + func.__name__
            + '_'
            + '_'.join(map(str, args))
            + '_'
            + '_'.join([f'{str(k)}__{str(v)}' for k, v in kwargs.items()])
            + '.snappy.parquet'
        )
        if os.path.isfile(file_name):
            log.info('Reading %s from cache', func.__name__)
            return pd.read_parquet(file_name)

        result = func(*args, **kwargs)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        log.info('Caching %s', func.__name__)
        result.to_parquet(file_name, compression='snappy')
        return result

    return inner_func
