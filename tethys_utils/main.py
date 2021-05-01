"""

"""
# import socket
# import os
# import io
# import numpy as np
# import zstandard as zstd
# import pickle
# import pandas as pd
# import copy
# import xarray as xr
# import boto3
# import botocore
# import smtplib
# import ssl
# import requests
# # from pandas.core.groupby import SeriesGroupBy, GroupBy
# import orjson
# from time import sleep
# import traceback
# from hashlib import blake2b
# from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats, StationBase
# # from data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats, StationBase
# from geojson import Point
# import urllib3
# from multiprocessing.pool import ThreadPool, Pool
# from tethysts.utils import key_patterns, get_object_s3, s3_connection, read_json_zstd, read_pkl_zstd
# from email.message import EmailMessage
# from datetime import date, datetime


####################################################
### Misc reference objects

# nc_ts_key_pattern = {'H23': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}/{station}.H23.nc',
                  # 'H25': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}.H25.nc'}

# nc_ts_key_pattern = {
#                     'H23': 'tethys/diff/{dataset_id}/{date}/{station_id}.H23.nc.zst',
#                     'H25': 'tethys/diff/{dataset_id}/{date}.H25.nc.zst'
#                     }



####################################################
### Mappings



#####################################################
### Functions











# def pd_groupby_fun(fun_name, df):
#     """
#     Function to make a function specifically to be used on pandas groupby objects from a string code of the associated function.
#     """
#     if isinstance(df, pd.Series):
#         fun1 = SeriesGroupBy.__dict__[fun_name]
#     elif isinstance(df, pd.DataFrame):
#         fun1 = GroupBy.__dict__[fun_name]
#     else:
#         raise ValueError('df should be either a Series or DataFrame.')
#     return fun1










#########################################
### Testing

# df = sites2.copy()
# lat_col = 'lat'
# lon_col = 'lon'
# lat = -45.623492
# lon = 168.271093
# coords = [lon, lat]
# site5 = Site(site_id='123', ref='Acton Stream at Hillas Road', virtual_site=False, geometry=geo2)

# dataset_id = '4d3e17b255dfbf4add8aed98'
# site_id = '0d9163fe8713c8cc3919dcf6'
# time_col = 'time'
# result_col = 'streamflow'
# qc_col = 'quality_code'
# precision = {'result': 4, 'qc': 0}
#
# virtual_site = False

# b2 = io.BytesIO()
#
# p_up1 = df_to_xarray(df4_up, nc_type, parameter, attrs1, encoding1, run_date_key, ancillary_variables, False)
#
#
# b1 = dataset_to_bytes(p_up1)
#
#
# p_up1.to_netcdf(b2, engine='netcdf4')



# from functools import partial
# from pandas.tseries.frequencies import to_offset
#
# def uround(t, freq):
#     freq = to_offset(freq)
#     return pd.Timestamp((t.value // freq.delta.value) * freq.delta.value)

# d1 = '269eda15b277ffd824c223fc'
# s1 = '59647c5fd331f9fad60e8df0'

# with open('/media/sdb1/Projects/git/tethys/tethys-extraction-es-hilltop/env-monitoring/data_dict.pkl', 'wb') as handle:
#     pickle.dump(data_dict, handle)

# with open('/media/sdb1/Projects/git/tethys/tethys-extraction-es-hilltop/env-monitoring/data_dict.pkl', 'rb') as handle:
#     data_dict = pickle.load(handle)

# write_pkl_zstd(data_dict, '/media/sdb1/Projects/git/tethys/tethys-extraction-es-hilltop/env-monitoring/data_dict.pkl.zstd')

# data_dict = read_pkl_zstd('/media/sdb1/Projects/git/tethys/tethys-extraction-es-hilltop/env-monitoring/data_dict.pkl.zstd')
