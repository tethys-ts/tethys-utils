# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:25:41 2019

@author: michaelek
"""
import pytest
from tethys_utils import *
import pandas as pd
from tethys_utils.datasets import get_path

pd.options.display.max_columns = 10


###############################################
### Parameters

d_name1 = '218810'

d_path1 = get_path(d_name1)

attrs = {'quality_code': {'standard_name': 'quality_flag',
  'long_name': 'NEMS quality code',
  'references': 'https://www.lawa.org.nz/media/16580/nems-quality-code-schema-2013-06-1-.pdf'},
 'well_depth': {'units': 'm'},
 'well_diameter': {'units': 'mm'},
 'well_screens': {'units': ''},
 'well_top_screen': {'units': 'm'},
 'well_bottom_screen': {'units': 'm'},
 'precipitation': {'feature': 'atmosphere',
  'parameter': 'precipitation',
  'method': 'sensor_recording',
  'processing_code': '1',
  'owner': 'ECan',
  'aggregation_statistic': 'cumulative',
  'frequency_interval': '1H',
  'utc_offset': '0H',
  'units': 'mm',
  'license': 'https://creativecommons.org/licenses/by/4.0/',
  'result_type': 'time_series',
  'standard_name': 'precipitation_amount'}}

encoding = {'quality_code': {'dtype': 'int16', '_FillValue': -9999},
 'well_depth': {'dtype': 'int32', '_FillValue': -99999, 'scale_factor': 0.1},
 'well_diameter': {'dtype': 'int32',
  '_FillValue': -99999,
  'scale_factor': 0.1},
 'well_screens': {'dtype': 'int16', '_FillValue': -9999},
 'well_top_screen': {'dtype': 'int32',
  '_FillValue': -99999,
  'scale_factor': 0.1},
 'well_bottom_screen': {'dtype': 'int32',
  '_FillValue': -99999,
  'scale_factor': 0.1},
 'precipitation': {'scale_factor': 0.1, 'dtype': 'int16', '_FillValue': -99}}

nc_type = 'H23'
param_name = 'precipitation'
run_date_key = '20200803T225843Z'
ancillary_variables = ['quality_code']
compression = True


########################################
### Tests


def test_read_pkl_zstd():
    df1 = read_pkl_zstd(d_path1)

    assert df1.shape == (20000, 7)

df1 = read_pkl_zstd(d_path1)


def test_write_pkl_zstd():
    p_df1 = write_pkl_zstd(df1)
    len1 = round(len(p_df1), -3)

    assert (len1 < 200000) and (len1 > 100000)


def test_df_to_xarray():
    p_ds1 = df_to_xarray(df1, nc_type, param_name, attrs, encoding, run_date_key, ancillary_variables, compression)
    len2 = round(len(p_ds1), -3)

    ds1 = df_to_xarray(df1, nc_type, param_name, attrs, encoding, run_date_key, ancillary_variables)

    assert (len(ds1) == 6) and (len2 < 30000) and (len2 > 20000)




# @pytest.mark.parametrize('input_sites', [input_sites1, input_sites2, input_sites3])
# def test_nat(input_sites):
#     f1 = FlowNat(from_date, to_date, input_sites=input_sites)
#
#     nat_flow = f1.naturalisation()
#
#     assert (len(f1.summ) >= 1) & (len(nat_flow) > 2900)
