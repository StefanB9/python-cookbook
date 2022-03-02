"""Tests for df_cache decorator"""
import datetime as dt
import os
import shutil
import unittest

import pandas as pd

from .df_cache import  df_cache


class TestDfCache(unittest.TestCase):
    """Test Class for df_cache decorator"""
    def setUp(self):
        get_test_df('arg')
        get_test_df(kw='kw')
        get_test_df('arg', kw='kw')
        get_test_df()

    def test_naming(self):
        """test if file name is as expected"""
        file_paths = {
            'cache/get_test_df_arg_.snappy.parquet',
            'cache/get_test_df__kw__kw.snappy.parquet',
            'cache/get_test_df_arg_kw__kw.snappy.parquet',
            f'cache/get_test_df__runtime__{dt.date.today().isoformat()}'
            f'.snappy.parquet'
        }
        for file_path in file_paths:
            self.assertTrue(os.path.isfile(file_path))

    def test_df_cache_result(self):
        """Test, that the cache works"""
        df_1 = get_test_df('argument', key='keyword_argument')
        df_2 = get_test_df('argument', key='keyword_argument')
        self.assertTrue(df_1.equals(df_2))

    def tearDown(self):
        shutil.rmtree('cache/')


@df_cache
def get_test_df(*arg, **kwargs):
    """Function to create cached dataframes"""
    if arg and not kwargs:
        return pd.DataFrame({'arg': [1, 2], 'no_kwargs': [3, 4]})
    if not arg and kwargs:
        return pd.DataFrame({'np_arg': [1, 2], 'kwargs': [3, 4]})
    if arg and kwargs:
        return pd.DataFrame({'arg': [1, 2], 'kwargs': [3, 4]})
    return pd.DataFrame({'no_arg': [1, 2], 'no_kwargs': [3, 4]})
