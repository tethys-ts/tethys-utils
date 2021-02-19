"""

"""
import socket
import os
import io
import numpy as np
import zstandard as zstd
import pickle
import pandas as pd
import copy
import xarray as xr
import boto3
import botocore
import smtplib
import ssl
import requests
from pandas.core.groupby import SeriesGroupBy, GroupBy
import orjson
from time import sleep
import traceback
from hashlib import blake2b
from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats, StationBase
# from data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats, StationBase
from geojson import Point
import urllib3
from multiprocessing.pool import ThreadPool, Pool
from tethysts.utils import key_patterns, get_object_s3
from email.message import EmailMessage


####################################################
### Misc reference objects

# nc_ts_key_pattern = {'H23': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}/{station}.H23.nc',
                  # 'H25': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}.H25.nc'}

nc_ts_key_pattern = {
                    'H23': 'tethys/diff/{dataset_id}/{date}/{station_id}.H23.nc.zst',
                    'H25': 'tethys/diff/{dataset_id}/{date}.H25.nc.zst'
                    }

base_ds_fields = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

####################################################
### Mappings

agg_stat_mapping = {'mean': 'mean', 'cumulative': 'sum', 'continuous': None, 'maximum': 'max', 'median': 'median', 'minimum': 'min', 'mode': 'mode', 'sporadic': None, 'standard_deviation': 'std', 'incremental': 'cumsum'}

#####################################################
### Functions


def s3_connection(conn_config, max_pool_connections=20):
    """
    Function to establish a connection with an S3 account. This can use the legacy connect (signature_version s3) and the curent version.

    Parameters
    ----------
    conn_config : dict
        A dictionary of the connection info necessary to establish an S3 connection.
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.

    Returns
    -------
    S3 client object
    """
    s3_config = copy.deepcopy(conn_config)

    if 'config' in s3_config:
        config0 = s3_config.pop('config')
        config0.update({'max_pool_connections': max_pool_connections})
        config1 = boto3.session.Config(**config0)

        s3_config1 = s3_config.copy()
        s3_config1.update({'config': config1})

        s3 = boto3.client(**s3_config1)
    else:
        s3_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections)})
        s3 = boto3.client(**s3_config)

    return s3


def list_parse_s3(s3_client, bucket, prefix, start_after='', delimiter='', continuation_token=''):
    """
    Wrapper S3 function around the list_objects_v2 base function with a Pandas DataFrame output.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    start_after : str
        The S3 key to start after.
    delimiter : str
        A delimiter is a character you use to group keys.
    continuation_token : str
        ContinuationToken indicates to S3 that the list is being continued on this bucket with a token.

    Returns
    -------
    DataFrame
    """
    if s3_client._endpoint.host == 'https://vault.revera.co.nz':
        js = []
        while True:
            js1 = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Marker=start_after, Delimiter=delimiter)

            if 'Contents' in js1:
                js.extend(js1['Contents'])
                if 'NextMarker' in js1:
                    start_after = js1['NextMarker']
                else:
                    break
            else:
                break

    else:

        js = []
        while True:
            js1 = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, StartAfter=start_after, Delimiter=delimiter, ContinuationToken=continuation_token)

            if 'Contents' in js1:
                js.extend(js1['Contents'])
                if 'NextContinuationToken' in js1:
                    continuation_token = js1['NextContinuationToken']
                else:
                    break
            else:
                break

    if js:
        f_df1 = pd.DataFrame(js)[['Key', 'LastModified', 'ETag', 'Size']].copy()
        try:
            f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.str.findall('\d\d\d\d\d\d\d\dT\d\d\d\d\d\dZ').apply(lambda x: x[0] if len(x) > 0 else np.nan), utc=True, errors='coerce').dt.tz_localize(None)
        except:
            # print('No dates to parse in Keys')
            f_df1['KeyDate'] = None
        f_df1['ETag'] = f_df1['ETag'].str.replace('"', '')
        f_df1['LastModified'] = pd.to_datetime(f_df1['LastModified']).dt.tz_localize(None)
    else:
        f_df1 = pd.DataFrame(columns=['Key', 'LastModified', 'ETag', 'Size', 'KeyDate'])

    return f_df1


def get_last_date(s3_df, default_date='1900-01-01', date_type='date'):
    """

    """
    if not s3_df.empty:
        last_run_date = s3_df['KeyDate'].max()
    else:
        last_run_date = pd.Timestamp(default_date)

    if date_type == 'str':
        last_run_date = last_run_date.strftime('%Y-%m-%d %H:%M:%S')

    return last_run_date


def make_run_date_key(run_date=None):
    """

    """
    if run_date is None:
        run_date = pd.Timestamp.today(tz='utc')
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')
    elif isinstance(run_date, pd.Timestamp):
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')
    elif isinstance(run_date, str):
        run_date_key = run_date
    else:
        raise TypeError('run_date must be None, Timestamp, or a string representation of a timestamp')

    return run_date_key


def write_json_zstd(data, compress_level=1):
    """
    Serializer of a python dictionary to json using orjson then compressed using zstandard.

    Parameters
    ----------
    data : dict
        A dictionary that can be serialized to json using orjson.
    compress_level : int
        zstandard compression level.

    Returns
    -------
    bytes object

    """
    json1 = orjson.dumps(data)
    cctx = zstd.ZstdCompressor(level=compress_level)
    c_obj = cctx.compress(json1)

    return c_obj


def read_json_zstd(obj):
    """
    Deserializer from a compressed zstandard json object to a dictionary.

    Parameters
    ----------
    obj : bytes
        The bytes object.

    Returns
    -------
    Dict
    """
    dctx = zstd.ZstdDecompressor()
    obj1 = dctx.decompress(obj)
    dict1 = orjson.loads(obj1)

    return dict1


def write_pkl_zstd(obj, file_path=None, compress_level=1, pkl_protocol=pickle.HIGHEST_PROTOCOL):
    """
    Serializer using pickle and zstandard. Converts any object that can be pickled to a binary object then compresses it using zstandard. Optionally saves the object to disk. If obj is bytes, then it will only be compressed without pickling.

    Parameters
    ----------
    obj : any
        Any pickleable object.
    file_path : None or str
        Either None to return the bytes object or a str path to save it to disk.
    compress_level : int
        zstandard compression level.

    Returns
    -------
    If file_path is None, then it returns the byte object, else None.
    """
    if isinstance(obj, bytes):
        p_obj = obj
    else:
        p_obj = pickle.dumps(obj, protocol=pkl_protocol)

    cctx = zstd.ZstdCompressor(level=compress_level)
    c_obj = cctx.compress(p_obj)

    if isinstance(file_path, str):
        with open(file_path, 'wb') as p:
            p.write(c_obj)
    else:
        return c_obj


def read_pkl_zstd(obj, unpickle=True):
    """
    Deserializer from a pickled object compressed with zstandard.

    Parameters
    ----------
    obj : bytes or str
        Either a bytes object that has been pickled and compressed or a str path to the file object.
    unpickle : bool
        Should the bytes object be unpickled or left as bytes?

    Returns
    -------
    Python object
    """
    dctx = zstd.ZstdDecompressor()
    if isinstance(obj, str):
        with open(obj, 'rb') as p:
            obj1 = dctx.decompress(p.read())
    elif isinstance(obj, bytes):
        obj1 = dctx.decompress(obj)
    else:
        raise TypeError('obj must either be a str path or a bytes object')

    if unpickle:
        obj1 = pickle.loads(obj1)

    return obj1


def put_s3_object(s3, bucket, obj, dataset_id, station_id, run_date=None, content_type='application/zstd'):
    """

    """
    run_date_key = make_run_date_key(run_date)
    ts_key = key_patterns['results'].format(dataset_id=dataset_id, station_id=station_id, run_date=run_date_key)
    s3.put_object(Body=obj, Bucket=bucket, Key=ts_key, ContentType=content_type, Metadata={'run_date': run_date_key})

    return ts_key


def results_data_integrety_checks(data, param_name, attrs, encoding, ancillary_variables=None):
    """

    """
    # Time series data
    data_cols = []
    if isinstance(ancillary_variables, list):
        if len(ancillary_variables) > 0:
            for av in ancillary_variables:
                if not av in data:
                    raise ValueError('The DataFrame must contain every value in the ancillary_variables list')
                else:
                    data_cols.extend([av])

    data_cols.extend([param_name])

    ts_index_list = ['time', 'height']
    ts_essential_list = [param_name]
    ts_no_attrs_list = ['modified_date']

    ts_data_cols = list(data.columns)

    for c in ts_essential_list:
        if not c in ts_data_cols:
            raise ValueError('The DataFrame must contain the column: ' + str(c))

    ts_data_index = list(data.index.names)

    for c in ts_index_list:
        if not c in ts_data_index:
            raise ValueError('The DataFrame must contain the index: ' + str(c))

    if isinstance(attrs, dict):
        attrs_keys = list(attrs.keys())
        for col in ts_data_cols:
            if not col in ts_no_attrs_list:
                if not col in ts_essential_list:
                    if not col in attrs_keys:
                        raise ValueError('Not all columns are in the attrs dict')
    else:
        raise TypeError('attrs must be a dict')

    if isinstance(encoding, dict):
        for col in ts_data_cols:
            if not col in ts_no_attrs_list:
                if not col in encoding:
                    raise ValueError(col + ' must be in the encoding dict')

    return data


# def station_data_integrety_checks(data, attrs=None, encoding=None):
#     """

#     """
#     # Station data
#     essentials = {'lat': (float, np.float), 'lon': (float, np.float), 'station_id': str, 'altitude': (int, float, np.float, np.int)}

#     for c in essentials:
#         if not c in data.keys():
#             raise ValueError('The station_data DataFrame must contain the field: ' + str(c))

#         if not isinstance(data[c], essentials[c]):
#             raise ValueError('The station_data DataFrame field must have the data type(s): ' + str(essentials[c]))

#     return data


def data_to_xarray(results_data, station_data, param_name, results_attrs, results_encoding, station_attrs=None, station_encoding=None, virtual_station=False, run_date=None, ancillary_variables=None, compression=False, compress_level=1):
    """
    Converts DataFrames of time series data, station data, and other attributes to an Xarray Dataset. Optionally has Zstandard compression.

    Parameters
    ----------
    results_data : DataFrame
        DataFrame of the core parameter and associated ancillary variable indexed by time and height.
        The index should have the names of "time" and "height". "height" is height above the surface. So if the parameter represents a surface measurement, then the height should be 0.
    station_data : dict
        Dictionary of the station data. Should include a station_id which should be a hashed string from blake2b (digest_size=12) of the geojson geometry. The minimum necessary other fields should include lat, lon, and altitude. Data owner specific other fields can include "ref" for the reference id and "name" for the station name.
    param_name : str
        The core parameter name of the column in the ts_data DataFrame.
    results_attrs : dict
        A dictionary of the xarray/netcdf attributes of the results_data. Where the keys are the columns and the values are the attributes.
    results_encoding : dict
        A dictionary of the xarray/netcdf encodings for the results_data.
    station_attrs : dict or None
        Similer to results_attr, but can be omitted if no extra fields are included in station_data.
    station_encoding : dict or None
        Similer to results_encoding, but can be omitted if no extra fields are included in station_data.

    Returns
    -------
    Xarray Dataset or bytes object

    """
    ## Integrity Checks

    # Time series data
    ts_data1 = results_data_integrety_checks(results_data, param_name, results_attrs, results_encoding, ancillary_variables)

    # Station data
    stn_m = StationBase(**station_data)
    stn_data = orjson.loads(stn_m.json(exclude_none=True))
    stn_data['lon'] = stn_data['geometry']['coordinates'][0]
    stn_data['lat'] = stn_data['geometry']['coordinates'][1]
    stn_data.pop('geometry')

    ## Assign Attributes

    if isinstance(station_attrs, dict):
        attrs1 = copy.deepcopy(station_attrs)
    else:
        attrs1 = {}

    attrs1.update({'station_id': {'cf_role': "timeseries_id", 'virtual_station': virtual_station}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'altitude': {'standard_name': 'surface_altitude', 'long_name': 'height above the geoid to the lower boundary of the atmosphere', 'units': 'm'}})
    if 'name' in stn_data.keys():
        attrs1.update({'name': {'long_name': 'station name'}})
    if 'ref' in stn_data.keys():
        attrs1.update({'ref': {'long_name': 'station reference id given by the owner'}})

    ts_cols = list(results_data.columns)

    attrs1.update(results_attrs)
    if 'cf_standard_name' in attrs1[param_name]:
        attrs1[param_name]['standard_name'] = attrs1[param_name].pop('cf_standard_name')
    attrs1.update({'height': {'standard_name': 'height', 'long_name': 'vertical distance above the surface', 'units': 'm', 'positive': 'up'}, 'time': {'standard_name': 'time', 'long_name': 'start_time'}})
    if 'modified_date' in ts_cols:
        attrs1.update({'modified_date': {'long_name': 'last modified date'}})

    if isinstance(ancillary_variables, list):
        if len(ancillary_variables) > 0:
            attrs1[param_name].update({'ancillary_variables': ' '.join(ancillary_variables)})

    ## Assign encodings
    if isinstance(station_encoding, dict):
        encoding1 = copy.deepcopy(station_encoding)
    else:
        encoding1 = {}

    encoding1.update({'lon': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.00001}, 'lat': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.00001}, 'altitude': {'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.001}})

    height = pd.to_numeric(results_data.reset_index()['height'], downcast='integer')

    if 'int' in height.dtype.name:
        height_enc = {'dtype': height.dtype.name, '_FillValue': -9999}
        dtype = height.dtype.name
    elif 'float' in height.dtype.name:
        height_enc = {'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.001}
    else:
        raise TypeError('height should be either an int or a float')

    encoding1.update(results_encoding)
    encoding1.update({'time': {'_FillValue': -99999999, 'units': "days since 1970-01-01 00:00:00"}, 'height': height_enc})
    if 'modified_date' in ts_cols:
        encoding1.update({'modified_date': {'_FillValue': -99999999, 'units': "days since 1970-01-01 00:00:00"}})

    ## Create the Xarray Dataset

    ds1 = results_data.to_xarray()
    for k, v in stn_data.items():
        ds1[k] = v

    ## Add attributes and encodings
    for e, val in encoding1.items():
        if e in ds1:
            if ('dtype' in val) and (not 'scale_factor' in val):
                if 'int' in val['dtype']:
                    ds1[e] = ds1[e].astype(val['dtype'])
            if 'scale_factor' in val:
                precision = int(np.abs(np.log10(val['scale_factor'])))
                ds1[e] = ds1[e].round(precision)
            ds1[e].encoding = val

    for a, val in attrs1.items():
        if a in ds1:
            ds1[a].attrs = val

    ds_mapping = results_attrs[param_name]
    title_str = '{agg_stat} {parameter} in {units} of the {feature} by a {method} owned by {owner}'.format(agg_stat=ds_mapping['aggregation_statistic'], parameter=ds_mapping['parameter'], units=ds_mapping['units'], feature=ds_mapping['feature'], method=ds_mapping['method'], owner=ds_mapping['owner'])

    run_date_key = make_run_date_key(run_date)
    ds1.attrs = {'featureType': 'timeSeries', 'title': title_str, 'institution': ds_mapping['owner'], 'license': ds_mapping['license'], 'source': ds_mapping['method'], 'history': run_date_key + ': Generated'}

    ## Test conversion to netcdf
    p_ts1 = ds1.to_netcdf()

    ## Compress if requested
    if compression:
        cctx = zstd.ZstdCompressor(level=compress_level)
        c_obj = cctx.compress(p_ts1)

        return c_obj
    else:
        return ds1


def grp_ts_agg(df, grp_col, ts_col, freq_code, agg_fun, discrete=False, **kwargs):
    """
    Simple function to aggregate time series with dataframes with a single column of stations and a column of times.

    Parameters
    ----------
    df : DataFrame
        Dataframe with a datetime column.
    grp_col : str, list of str, or None
        Column name that contains the stations.
    ts_col : str
        The column name of the datetime column.
    freq_code : str
        The pandas frequency code for the aggregation (e.g. 'M', 'A-JUN').
    discrete : bool
        Is the data discrete? Will use proper resampling using linear interpolation.

    Returns
    -------
    Pandas DataFrame
    """

    df1 = df.copy()
    if isinstance(df, pd.DataFrame):
        if df[ts_col].dtype.name == 'datetime64[ns]':
            df1.set_index(ts_col, inplace=True)
            if grp_col is None:

                if discrete:
                    df3 = discrete_resample(df1, freq_code, agg_fun, **kwargs)
                else:
                    df3 = df1.resample(freq_code, **kwargs).agg(agg_fun)

                return df3

            elif isinstance(grp_col, str):
                grp_col = [grp_col]
            elif isinstance(grp_col, list):
                grp_col = grp_col[:]
            else:
                raise TypeError('grp_col must be a str, list, or None')

            if discrete:
                val_cols = [c for c in df1.columns if c not in grp_col]

                grp1 = df1.groupby(grp_col)

                grp_list = []

                for i, r in grp1:
                    s6 = discrete_resample(r[val_cols], freq_code, agg_fun, **kwargs)
                    s6[grp_col] = i

                    grp_list.append(s6)

                df2 = pd.concat(grp_list)
                df2.index.name = ts_col

                df3 = df2.reset_index().set_index(grp_col + [ts_col]).sort_index()

            else:
                grp_col.extend([pd.Grouper(freq=freq_code, **kwargs)])
                df3 = df1.groupby(grp_col).agg(agg_fun)

            return df3

        else:
            raise ValueError('Make one column a timeseries!')
    else:
        raise TypeError('The object must be a DataFrame')


def compare_dfs(old_df, new_df, on, parameter, add_old=False):
    """
    Function to compare two DataFrames with nans and return a dict with rows that have changed (diff), rows that exist in new_df but not in old_df (new), and rows  that exist in old_df but not in new_df (remove).
    Both DataFrame must have the same columns. If both DataFrames are identical, and empty DataFrame will be returned.

    Parameters
    ----------
    old_df : DataFrame
        The old DataFrame.
    new_df : DataFrame
        The new DataFrame.
    on : str or list of str
        The primary key(s) to index/merge the two DataFrames.
    parameter : str
        The parameter/column that should be compared.

    Returns
    -------
    DataFrame
        of the new dataset
    """
    if ~np.in1d(old_df.columns, new_df.columns).any():
        raise ValueError('Both DataFrames must have the same columns')

    # val_cols = [c for c in old_df.columns if not c in on]
    all_cols = new_df.columns.tolist()

    comp1 = pd.merge(old_df, new_df, on=on, how='outer', indicator=True, suffixes=('_x', ''))

    add_set = comp1.loc[comp1._merge == 'right_only', all_cols].copy()
    comp2 = comp1[comp1._merge == 'both'].drop('_merge', axis=1).copy()

    old_cols = list(on)
    old_cols_map = {c: c[:-2] for c in comp2 if '_x' in c}
    old_cols.extend(old_cols_map.keys())
    old_set = comp2[old_cols].copy()
    old_set.rename(columns=old_cols_map, inplace=True)
    new_set = comp2[all_cols].copy()

    isnull1 = new_set[parameter].isnull()
    if isnull1.any():
        new_set.loc[new_set[parameter].isnull(), parameter] = np.nan
    if old_set[parameter].dtype.type in (np.float32, np.float64):
        c1 = ~np.isclose(old_set[parameter], new_set[parameter], equal_nan=True)
    elif old_set[parameter].dtype.name == 'object':
        new_set[parameter] = new_set[parameter].astype(str)
        c1 = old_set[parameter].astype(str) != new_set[parameter]
    elif old_set[parameter].dtype.name == 'geometry':
        old1 = old_set[parameter].apply(lambda x: hash(x.wkt))
        new1 = new_set[parameter].apply(lambda x: hash(x.wkt))
        c1 = old1 != new1
    else:
        c1 = old_set[parameter] != new_set[parameter]
    notnan1 = old_set[parameter].notnull() | new_set[parameter].notnull()
    c2 = c1 & notnan1

    if (len(comp1) == len(comp2)) and (~c2).all():
        all_set = pd.DataFrame()
    else:
        diff_set = new_set[c2].copy()
        old_set2 = old_set[~c2].copy()

        if add_old:
            not_cols = list(on)
            [not_cols.extend([c]) for c in comp1.columns if '_x' in c]
            add_old1 = comp1.loc[comp1._merge == 'left_only', not_cols].copy()
            add_old1.rename(columns=old_cols_map, inplace=True)

            all_set = pd.concat([old_set2, diff_set, add_set, add_old1])
        else:
            all_set = pd.concat([old_set2, diff_set, add_set])

    return all_set


def compare_xrs(old_xr, new_xr, add_old=False):
    """

    """
    ## Determine the parameter to be compared and the dimensions
    vars1 = list(new_xr.variables)
    parameter = [v for v in vars1 if 'dataset_id' in new_xr[v].attrs][0]
    vars2 = [parameter]

    if not parameter in old_xr:
        raise ValueError(parameter + ' must be in old_xr')

    on = new_xr[parameter].dims

    if not on == old_xr[parameter].dims:
        raise ValueError('Dimensions are not the same between the datasets')

    ## Determine if there are ancillary variables to pull through
    new_attrs = new_xr[parameter].attrs.copy()

    if 'ancillary_variables' in new_attrs:
        av1 = new_attrs['ancillary_variables'].split(' ')
        vars2.extend(av1)

    ## Pull out data for comparison
    old_df = old_xr[vars2].to_dataframe().reset_index()
    new_df = new_xr[vars2].to_dataframe().reset_index()

    # old_df['modified_date'] = pd.Timestamp('2020-12-29')

    ## run comparison
    comp = compare_dfs(old_df, new_df, on, parameter, add_old=add_old)

    if comp.empty:
        print('Nothing has changed. Returning empty DataFrame.')
        return comp

    else:

        ## Repackage into netcdf
        comp2 = comp.set_index(list(on)).sort_index().to_xarray()

        for v in vars1:
            if v in comp2:
                comp2[v].attrs = new_xr[v].attrs.copy()
                comp2[v].encoding = new_xr[v].encoding.copy()
            else:
                comp2[v] = new_xr[v].copy()

        comp2.attrs = new_xr.attrs.copy()

        return comp2


def discrete_resample(df, freq_code, agg_fun, remove_inter=False, **kwargs):
    """
    Function to properly set up a resampling class for discrete data. This assumes a linear interpolation between data points.

    Parameters
    ----------
    df: DataFrame or Series
        DataFrame or Series with a time index.
    freq_code: str
        Pandas frequency code. e.g. 'D'.
    agg_fun : str
        The aggregation function to be applied on the resampling object.
    **kwargs
        Any keyword args passed to Pandas resample.

    Returns
    -------
    Pandas DataFrame or Series
    """
    if isinstance(df, (pd.Series, pd.DataFrame)):
        if isinstance(df.index, pd.DatetimeIndex):
            reg1 = pd.date_range(df.index[0].ceil(freq_code), df.index[-1].floor(freq_code), freq=freq_code)
            reg2 = reg1[~reg1.isin(df.index)]
            if isinstance(df, pd.Series):
                s1 = pd.Series(np.nan, index=reg2)
            else:
                s1 = pd.DataFrame(np.nan, index=reg2, columns=df.columns)
            s2 = pd.concat([df, s1]).sort_index()
            s3 = s2.interpolate('time')
            s4 = (s3 + s3.shift(-1))/2
            s5 = s4.resample(freq_code, **kwargs).agg(agg_fun).dropna()

            if remove_inter:
                index1 = df.index.floor(freq_code).unique()
                s6 = s5[s5.index.isin(index1)].copy()
            else:
                s6 = s5
        else:
            raise ValueError('The index must be a datetimeindex')
    else:
        raise TypeError('The object must be either a DataFrame or a Series')

    return s6


def email_msg(sender_address, sender_password, receiver_address, subject, body, smtp_server="smtp.gmail.com"):
    """
    Function to send a simple email using gmail smtp.

    Parameters
    ----------
    sender_address : str
        The email address of the account that is sending the email.
    sender_password : str
        The password of the sender account.
    receiver_address : str or list of str
        The email addresses of the recipients.
    subject: str
        The subject of the email.
    body : str
        The main body of the email.
    smtp_server : str
        The SMTP server to send the email through.

    Returns
    -------
    None

    """
    port = 465  # For SSL

    msg = EmailMessage()

    # msg_base = """From: {from1}\n
    # To: {to}\n
    # Subject: {subject}\n

    # {body}

    # hostname: {host}
    # IP address: {ip}"""

    ip = requests.get('https://api.ipify.org').text

    body_base = """{body}

    hostname: {host}
    IP address: {ip}"""

    # msg = msg_base.format(subject=subject, body=body, host=socket.getfqdn(), ip=ip, from1=sender_address, to=receiver_address)

    msg['From'] = sender_address
    msg['To'] = receiver_address
    msg['Subject'] = subject
    msg.set_content(body_base.format(body=body, host=socket.getfqdn(), ip=ip))

    # Create a secure SSL context
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_address, sender_password)
        server.send_message(msg)


def tsreg(ts, freq=None, interp=False):
    """
    Function to regularize a time series object (pandas).
    The first three indeces must be regular for freq=None!!!

    Parameters
    ----------
    ts : DataFrame
        pandas time series dataframe.
    freq : str or None
        Either specify the known frequency of the data or use None and
    determine the frequency from the first three indices.
    interp : bool
        Should linear interpolation be applied on all missing data?

    Returns
    -------
    DataFrame
    """

    if freq is None:
        freq = pd.infer_freq(ts.index[:3])
    ts1 = ts.resample(freq).mean()
    if interp:
        ts1 = ts1.interpolate('time')

    return ts1


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

def assign_ds_ids(datasets):
    """
    Parameters
    ----------
    datasets : list
    """
    dss = copy.deepcopy(datasets)

    ### Iterate through the dataset list
    for ds in dss:
        # print(ds)
        ## Validate base model
        ds_m = DatasetBase(**ds)

        base_ds = {k: ds[k] for k in base_ds_fields}
        base_ds_b = orjson.dumps(base_ds)
        ds_id = blake2b(base_ds_b, digest_size=12).hexdigest()

        ds['dataset_id'] = ds_id

        ## Validate full model
        ds_m = Dataset(**ds)

    return dss


def process_datasets(datasets):
    """

    """
    for ht_ds, ds_list in datasets.items():
        ds_list2 = assign_ds_ids(ds_list)
        datasets[ht_ds] = ds_list2

    dataset_list = []
    [dataset_list.extend(ds_list) for ht_ds, ds_list in datasets.items()]

    return dataset_list



def put_remote_dataset(s3, bucket, dataset, run_date=None):
    """

    """
    run_date_key = make_run_date_key(run_date)

    dataset_id = dataset['dataset_id']

    ds4 = Dataset(**dataset)

    ds5 = orjson.loads(ds4.json(exclude_none=True))

    ds_obj = write_json_zstd(ds5)

    ds_key = key_patterns['dataset'].format(dataset_id=dataset_id)

    obj2 = s3.put_object(Bucket=bucket, Key=ds_key, Body=ds_obj, Metadata={'run_date': run_date_key}, ContentType='application/json')

    return ds5


# def update_remote_dataset(s3, bucket, dataset_id, dataset, run_date=None):
#     """
#
#     """
#     run_date_key = make_run_date_key(run_date)
#
#     ds_key = key_patterns['dataset']
#     try:
#         obj1 = s3.get_object(Bucket=bucket, Key=ds_key)
#         rem_ds_body = obj1['Body']
#         rem_ds = orjson.loads(rem_ds_body.read())
#     except:
#         rem_ds = []
#
#     if rem_ds != datasets:
#         print('datasets are different, datasets.json will be updated')
#         all_ids = set([i['dataset_id'] for i in rem_ds])
#         all_ids.update(set([i['dataset_id'] for i in datasets]))
#
#         up_list = copy.deepcopy(rem_ds)
#         up_list = []
#
#         for i in all_ids:
#             rem1 = [n for n in rem_ds if n['dataset_id'] == i]
#             new1 = [n for n in datasets if n['dataset_id'] == i]
#
#             if new1:
#                 up_list.append(new1[0])
#             else:
#                 up_list.append(rem1[0])
#
#         ds_json1 = orjson.dumps(up_list)
#         obj2 = s3.put_object(Bucket=bucket, Key=ds_key, Body=ds_json1, Metadata={'run_date': run_date_key}, ContentType='application/json')
#     else:
#         print('datasets are the same, datasets.json will not be updated')
#         up_list = rem_ds
#
#     return up_list


def create_geometry(coords, geo_type='Point'):
    """

    """
    if geo_type == 'Point':
        coords = [round(coords[0], 5), round(coords[1], 5)]
        geo1 = Point(coords)
    else:
        raise ValueError('geo_type not implemented yet')

    if not geo1.is_valid:
        raise ValueError('coordinates are not valid')

    geo2 = Geometry(**geo1).dict()

    return geo2


def assign_station_id(geometry):
    """

    """
    station_id = blake2b(orjson.dumps(geometry), digest_size=12).hexdigest()

    return station_id


def get_new_stats(data):
    """

    """
    vars1 = list(data.variables)
    parameter = [v for v in vars1 if 'dataset_id' in data[v].attrs][0]
    precision = int(np.abs(np.log10(data[parameter].attrs['precision'])))

    min1 = round(float(data[parameter].min()), precision)
    max1 = round(float(data[parameter].max()), precision)
    from_date = pd.Timestamp(data['time'].min().values).tz_localize(None)
    to_date = pd.Timestamp(data['time'].max().values).tz_localize(None)

    stats1 = Stats(min=min1, max=max1, from_date=from_date, to_date=to_date)

    return stats1


def process_object_keys(s3, bucket, prefix):
    """

    """
    keys1 = list_parse_s3(s3, bucket, prefix)
    keys2 = keys1[keys1.Key.str.contains('results')]

    infos1 = [S3ObjectKey(key=row['Key'], bucket=bucket, content_length=row['Size'], etag=row['ETag'], run_date=row['KeyDate'], modified_date=row['LastModified']) for i, row in keys2.iterrows()]

    return infos1


def process_stations_df(stns_df):
    """
    For processing the initial stations prior to assigning the altitude.
    """
    stns1 = stns_df.copy()
    stns1['coords'] = stns1.apply(lambda x: [x.lon, x.lat], axis=1)

    stns1['geo'] = stns1.apply(lambda x: create_geometry(x.coords), axis=1)
    stns1['station_id'] = stns1.apply(lambda x: assign_station_id(x.geo), axis=1)

    stns2 = stns1.drop_duplicates('station_id').drop('geo', axis=1).copy()

    return stns2


def process_station_base(coords, ref=None, name=None, osm_id=None, altitude=None, properties=None, virtual_station=False, geo_type='Point', return_dict=True):
    """

    """
    ## Create geometry and station_id
    geo = create_geometry(coords, geo_type=geo_type)
    stn_id = assign_station_id(geo)

    ## Put into data model
    stn_m = StationBase(station_id=stn_id, geometry=geo, ref=ref, name=name, osm_id=osm_id, altitude=altitude, properties=properties, virtual_station=virtual_station)

    if return_dict:
        stn_m = orjson.loads(stn_m.json(exclude_none=True))

    return stn_m


def process_stations_base(stns_list):
    """

    """
    stns_dict = {}
    for s in stns_list:
        stn_m = process_station_base(**s)
        stns_dict[stn_m['station_id']] = stn_m

    return stns_dict


def get_station_data_from_xr(data):
    """
    Parameters
    ----------
    data : xr.Dataset
    """
    vars1 = list(data.variables)
    dims1 = list(data.dims.keys())
    parameter = [v for v in vars1 if 'dataset_id' in data[v].attrs][0]
    attrs = data[parameter].attrs.copy()
    data_vars = [parameter]
    if 'ancillary_variables' in attrs:
        ancillary_variables = attrs['ancillary_variables'].split(' ')
        data_vars.extend(ancillary_variables)

    stn_vars = [v for v in vars1 if (not v in dims1) and (not v in data_vars)]
    stn_data1 = {k: v['data'] for k, v in data[stn_vars].to_dict()['data_vars'].items()}
    stn_data1['geometry'] = create_geometry([stn_data1['lon'], stn_data1['lat']])
    # stn_data1['lon'] = stn_data1['geometry']['coordinates'][0]
    # stn_data1['lat'] = stn_data1['geometry']['coordinates'][1]
    stn_data1.pop('lon')
    stn_data1.pop('lat')
    stn_data1['altitude'] = round(stn_data1['altitude'], 3)

    return stn_data1


def process_station_summ(dataset_id, data, connection_config, bucket, mod_date=None):
    """

    """
    if mod_date is None:
        mod_date = pd.Timestamp.today(tz='utc').round('s').tz_localize(None)
    elif isinstance(mod_date, (str, pd.Timestamp)):
        mod_date = pd.Timestamp(mod_date).tz_localize(None)

    ## Genreate the info for the recently created data
    station_id = str(data['station_id'].values)
    stats1 = get_new_stats(data)

    s3 = s3_connection(connection_config)
    prefix = key_patterns['results'].split('{run_date}')[0].format(dataset_id=dataset_id, station_id=station_id)

    object_infos1 = process_object_keys(s3, bucket, prefix)

    ## Get old data
    stn_key = key_patterns['station'].format(dataset_id=dataset_id, station_id=station_id)

    try:
        old_stn_data_obj = get_object_s3(stn_key, connection_config, bucket)
        old_stn_data = read_json_zstd(old_stn_data_obj)
    except:
        old_stn_data = {}

    if old_stn_data:
        old_stats = old_stn_data['stats']
        min1 = min([old_stats['min'], stats1.min])
        max1 = max([old_stats['max'], stats1.max])
        to_date = max([pd.Timestamp(old_stats['to_date']).tz_localize(None), stats1.to_date])
        from_date = min([pd.Timestamp(old_stats['from_date']).tz_localize(None), stats1.from_date])

        stats2 = Stats(min=min1, max=max1, from_date=from_date, to_date=to_date)
    else:
        stats2 = stats1

    ## Put it all together
    stn_dict2 = get_station_data_from_xr(data)
    stn_dict2.update({'dataset_id': dataset_id, 'stats': stats2, 'results_object_key': object_infos1, 'modified_date': mod_date})

    station_m = Station(**stn_dict2)

    return station_m


def get_remote_dataset(s3, bucket, dataset_id=None, ds_key=None):
    """

    """
    if isinstance(dataset_id, str):
        ds_key = key_patterns['dataset'].format(dataset_id=dataset_id)

    try:
        obj1 = s3.get_object(Bucket=bucket, Key=ds_key)
        rem_ds_body = obj1['Body']
        jzstd = rem_ds_body.read()
        rem_ds = read_json_zstd(jzstd)
    except:
        rem_ds = None

    return rem_ds


def get_remote_station(s3, bucket, dataset_id=None, station_id=None, stn_key=None):
    """

    """
    if isinstance(dataset_id, str):
        stn_key = key_patterns['station'].format(dataset_id=dataset_id, station_id=station_id)

    try:
        obj1 = s3.get_object(Bucket=bucket, Key=stn_key)
        rem_stn_body = obj1['Body']
        jzstd = rem_stn_body.read()
        rem_stn = read_json_zstd(jzstd)
    except:
        rem_stn = None

    return rem_stn


def put_remote_station(s3, bucket, station, run_date=None):
    """

    """
    run_date_key = make_run_date_key(run_date)

    dataset_id = station['dataset_id']
    station_id = station['station_id']

    stn4 = Station(**station)

    stn5 = orjson.loads(stn4.json(exclude_none=True))

    stn_obj = write_json_zstd(stn5)

    stn_key = key_patterns['station'].format(dataset_id=dataset_id, station_id=station_id)

    obj2 = s3.put_object(Bucket=bucket, Key=stn_key, Body=stn_obj, Metadata={'run_date': run_date_key}, ContentType='application/json')

    return stn5


def put_remote_agg_stations(s3, bucket, dataset_id, threads=20):
    """

    """
    base_stn_key = key_patterns['station']
    agg_stn_key = key_patterns['stations']

    stn_prefix = agg_stn_key.split('stations.json.zst')[0].format(dataset_id=dataset_id)

    list1 = list_parse_s3(s3, bucket, stn_prefix)
    list2 = list1[list1.Key.str.contains('station.json.zst')].copy()

    # stn_list = [{'s3': s3, 'bucket': bucket, 'dataset_id': None, 'stn_key': k} for k in list2.Key]
    stn_list = [[s3, bucket, None, None, k] for k in list2.Key]

    output = ThreadPool(threads).starmap(get_remote_station, stn_list)

    stns_obj = write_json_zstd(output)

    run_date_key = make_run_date_key()
    stns_key = agg_stn_key.format(dataset_id=dataset_id)
    s3.put_object(Body=stns_obj, Bucket=bucket, Key=stns_key, ContentType='application/json', Metadata={'run_date': run_date_key})

    return output


def put_remote_agg_datasets(s3, bucket, threads=20):
    """

    """
    base_ds_key = key_patterns['dataset']
    agg_ds_key = key_patterns['datasets']

    ds_prefix = agg_ds_key.split('datasets.json.zst')[0]

    list1 = list_parse_s3(s3, bucket, ds_prefix)
    list2 = list1[list1.Key.str.contains('dataset.json.zst')].copy()

    # ds_list = [{'s3': s3, 'bucket': bucket, 'dataset_id': None, 'ds_key': k} for k in list2.Key]
    ds_list = [[s3, bucket, None, k] for k in list2.Key]

    output = ThreadPool(threads).starmap(get_remote_dataset, ds_list)

    dss_obj = write_json_zstd(output)

    run_date_key = make_run_date_key()
    dss_key = agg_ds_key
    s3.put_object(Body=dss_obj, Bucket=bucket, Key=dss_key, ContentType='application/json', Metadata={'run_date': run_date_key})

    return output


def compare_datasets_from_s3(conn_config, bucket, new_data, add_old=False, read_buffer=False, last_run_date_key=None, public_url=None):
    """
    Parameters
    ----------
    conn_config : dict
        A dictionary of the connection info necessary to establish an S3 connection.
    bucket : str
        The S3 bucket.
    new_data : xr.Dataset
        The new data that should be compared to existing data in S3.
    add_old : bool
        Should the data in the S3 be added to the output?
    read_buffer : bool
        Should the results buffer file be read instead of the normal results file?
    last_run_date_key : str
        Specify the last run key instead of having the function figure it out. The function will do a check to make sure that the key exists.
    public_url : str
        Optional if there is a public URL to the object instead of using the S3 API directly.

    Returns
    -------
    xr.Dataset
        Of the data that should be updated.
    """
    ## Determine the parameter, station_id, and dataset_id
    vars1 = list(new_data.variables)
    dataset = [new_data[v].attrs for v in vars1 if 'dataset_id' in new_data[v].attrs][0]
    dataset_id = dataset['dataset_id']
    # result_type = dataset['result_type']
    station_id = str(new_data['station_id'].values)

    key_dict = {'dataset_id': dataset_id, 'station_id': station_id}

    if read_buffer:
        base_key_pattern = key_patterns['results_buffer']
    else:
        base_key_pattern = key_patterns['results']

    ## Get list of keys
    s3 = s3_connection(conn_config)

    if isinstance(last_run_date_key, str):
        key_dict.update({'run_date': last_run_date_key})
        last_key = base_key_pattern.format(**key_dict)
        try:
            last_info1 = s3.head_object(Bucket=bucket, Key=last_key)
            last_key1 = pd.DataFrame({'Key': [last_key]})
        except:
            last_key1 = pd.DataFrame()
    else:
        prefix_key = key_patterns['results'].split('{run_date}')[0].format(**key_dict)
        all_keys = list_parse_s3(s3, bucket, prefix_key)
        last_key1 = all_keys[all_keys['KeyDate'] == all_keys['KeyDate'].max()]

    ## Get previous data and compare
    if isinstance(public_url, str):
        conn_config = public_url

    if not last_key1.empty:
        last_key = last_key1.iloc[0]['Key']
        p_old_one = get_object_s3(last_key, conn_config, bucket, 'zstd')
        xr_old_one = xr.open_dataset(p_old_one)
        xr_old_one['time'] = xr_old_one['time'].dt.round('s')

        up1 = compare_xrs(xr_old_one, new_data, add_old=add_old)
    else:
        print('No prior data found in S3. All data will be returned.')
        up1 = new_data.copy()

    return up1



def get_filtered_obj_list(remote, dataset_list):
    """

    """
    base_prefix = key_patterns['results'].split('{dataset_id}')[0]
    dataset_ids = [d['dataset_id'] for d in dataset_list]

    s3 = s3_connection(remote['connection_config'])
    obj_df = list_parse_s3(s3, remote['bucket'], base_prefix)

    obj_df1 = obj_df[obj_df['KeyDate'].notnull()].copy()
    obj_df1['dataset_id'] = obj_df1['Key'].apply(lambda x: x.split('/')[2])
    obj_df1['station_id'] = obj_df1['Key'].apply(lambda x: x.split('/')[3])
    obj_df1['result_type'] = 'results'
    obj_df1.loc[obj_df1['Key'].str.contains('buffer'), 'result_type'] = 'buffer'

    obj_df2 = obj_df1[obj_df1['dataset_id'].isin(dataset_ids)].copy()

    return obj_df2


def get_last_results(obj_df):
    """

    """
    last_date1 = obj_df.groupby(['dataset_id', 'station_id'])['KeyDate'].last().reset_index()
    obj_df1 = pd.merge(last_date1, obj_df[['Key', 'dataset_id', 'station_id', 'KeyDate', 'result_type']], on=['dataset_id', 'station_id', 'KeyDate'])

    return obj_df1


def filter_old_ones(obj_df, run_date, days_prior):
    """

    """
    if isinstance(run_date.tzname(), str):
        run_date1 = run_date.tz_convert('UTC').tz_localize(None)
    else:
        run_date1 = run_date

    old_date = run_date1 - pd.DateOffset(days=days_prior)

    obj_df1 = obj_df[obj_df['KeyDate'] < old_date].copy()

    return obj_df1


def process_buffer(row, remote, run_date_key):
    """

    """
    data_list = []
    for i, r in row.iterrows():
        obj1 = get_object_s3(r['Key'], remote['connection_config'], remote['bucket'], 'zstd')
        b1 = io.BytesIO(obj1)
        xr1 = xr.open_dataset(b1)
        data_list.append(xr1)

    xr2 = xr.concat(data_list, dim='time', data_vars='minimal')
    xr3 = xr2.sel(time=~xr2.get_index('time').duplicated('last'))
    xr3.attrs.update({'history': run_date_key + ': Generated'})

    key_dict = {'dataset_id': r['dataset_id'], 'station_id': r['station_id'], 'run_date': run_date_key}
    new_key = key_patterns['results'].format(**key_dict)

    cctx = zstd.ZstdCompressor(level=1)
    c_obj = cctx.compress(xr3.to_netcdf())

    s3 = s3_connection(remote['connection_config'])

    s3.put_object(Body=c_obj, Bucket=remote['bucket'], Key=new_key, ContentType='application/zstd', Metadata={'run_date': run_date_key})


def process_buffer_threaded(obj_df, remote, run_date_key, threads=30):
    """

    """
    grp1 = obj_df.groupby(['dataset_id', 'station_id'])

    input_list = [[row, remote, run_date_key] for i, row in grp1]

    output = ThreadPool(threads).starmap(process_buffer, input_list)


def prepare_station_results(data_dict, dataset_list, station_dict, data_df, run_date_key, mod_date=None, sum_closed='right', other_closed='left', discrete=True):
    """

    """
    if isinstance(mod_date, (str, pd.Timestamp)):
        mod_date = pd.Timestamp(mod_date)
        ancillary_variables = ['modified_date']
    else:
        ancillary_variables = []
    #     mod_date = pd.Timestamp.today(tz='utc').round('s').tz_localize(None)

    ts_data1 = data_df.copy()

    ## Iterate through each dataset
    for ds in dataset_list:
        print(ds['dataset_id'])

        ds_mapping = copy.deepcopy(ds)
        properties = ds_mapping.pop('properties')
        attrs = properties['attrs']
        encoding = properties['encoding']

        attrs1 = copy.deepcopy(attrs)
        attrs1.update({ds_mapping['parameter']: ds_mapping})

        encoding1 = copy.deepcopy(encoding)

        ## Pre-Process data
        qual_col = 'quality_code'
        freq_code = ds_mapping['frequency_interval']
        parameter = ds_mapping['parameter']

        ## Aggregate data if necessary
        # Parameter
        if freq_code == 'T':
            grp1 = ts_data1.groupby(['time', 'height'])
            data1 = grp1[parameter].mean()

        else:
            agg_fun = agg_stat_mapping[ds_mapping['aggregation_statistic']]

            if agg_fun == 'sum':
                data1 = grp_ts_agg(ts_data1[['time', 'height', parameter]], 'height', 'time', freq_code, agg_fun, closed=sum_closed)
            else:
                data1 = grp_ts_agg(ts_data1[['time', 'height', parameter]], 'height', 'time', freq_code, agg_fun, discrete, closed=other_closed)

        # Quality code
        if qual_col in ts_data1.columns:
            ts_data1[qual_col] = pd.to_numeric(ts_data1[qual_col], errors='coerce', downcast='integer')
            if freq_code == 'T':
                qual1 = grp1[qual_col].min()
            else:
                qual1 = grp_ts_agg(ts_data1[['time', 'height', qual_col]], 'height', 'time', freq_code, 'min')
            df3 = pd.concat([data1, qual1], axis=1).reset_index().dropna()

            ancillary_variables.extend([qual_col])
        else:
            df3 = data1.reset_index().copy()

        if 'modified_date' in ancillary_variables:
            df3['modified_date'] = mod_date

        ## Convert to xarray
        df4 = df3.copy()
        df4.set_index(['time', 'height'], inplace=True)

        new1 = data_to_xarray(df4, station_dict, parameter, attrs1, encoding1, run_date=run_date_key, ancillary_variables=ancillary_variables, compression='zstd')

        ## Update the data_dict
        ds_id = ds_mapping['dataset_id']
        stn_id = station_dict['station_id']

        data_dict[ds_id].append(new1)


def update_results_s3(data_dict, conn_config, bucket, threads=10, add_old=False, read_buffer=False, last_run_date_key=None, public_url=None):
    """
    Parameters
    ----------
    data_dict : dict of lists
        A dictionary with the keys as the dataset_ids and teh values as lists of zstd compressed xr.Datasets.
    conn_config : dict
        A dictionary of the connection info necessary to establish an S3 connection.
    bucket : str
        The S3 bucket.
    threads : int
        The number of threads to use to process the data.
    add_old : bool
        Should the data in the S3 be added to the output?
    read_buffer : bool
        Should the results buffer file be read instead of the normal results file?
    last_run_date_key : str
        Specify the last run key instead of having the function figure it out. The function will do a check to make sure that the key exists.
    public_url : str
        Optional if there is a public URL to the object instead of using the S3 API directly.

    Returns
    -------
    None

    """
    s3 = s3_connection(conn_config, threads)

    for ds_id, results in data_dict.items():
        print('--dataset_id: ' + ds_id)

        def update_result(result):
            """

            """
            ## Process data
            try:
                new1 = xr.load_dataset(read_pkl_zstd(result, False))
            except:
                print('Data could not be opened')
                return None

            stn_id = str(new1['station_id'].values)
            print('station_id: ' + stn_id)

            vars1 = list(new1.variables)
            parameter = [v for v in vars1 if 'dataset_id' in new1[v].attrs][0]
            attrs = new1[parameter].attrs.copy()

            ds_id = attrs['dataset_id']
            run_date_key = new1.attrs['history'].split(':')[0]

            up1 = compare_datasets_from_s3(conn_config, bucket, new1, add_old=add_old, read_buffer=read_buffer, last_run_date_key=last_run_date_key, public_url=public_url)

            ## Save results
            if isinstance(up1, xr.Dataset) and (len(up1[parameter].time) > 0):

                print('Save results')
                key_dict = {'dataset_id': ds_id, 'station_id': stn_id, 'run_date': run_date_key}

                new_key = key_patterns['results'].format(**key_dict)

                cctx = zstd.ZstdCompressor(level=1)
                c_obj = cctx.compress(up1.to_netcdf())

                s3.put_object(Body=c_obj, Bucket=bucket, Key=new_key, ContentType='application/zstd', Metadata={'run_date': run_date_key})

                ## Process stn data
                print('Save station data')

                stn_m = process_station_summ(ds_id, up1, conn_config, bucket, mod_date=run_date_key)

                stn4 = orjson.loads(stn_m.json(exclude_none=True))
                up_stns = put_remote_station(s3, bucket, stn4, run_date=run_date_key)

            else:
                print('No new data to update')

            ## Get rid of big objects
            new1 = None
            up1 = None


        ## Run the threadpool
        # output = ThreadPool(threads).imap_unordered(update_result, results)
        with ThreadPool(threads) as pool:
            output = pool.map(update_result, results)
            pool.close()
            pool.join()



def delete_result_objects_s3(conn_config, bucket, dataset_ids=None, keep_last=10):
    """
    Parameters
    ----------
    conn_config : dict
        A dictionary of the connection info necessary to establish an S3 connection.
    bucket : str

    dataset_ids : list or str
        The specific datasets that should have the results objects removed. None will remove results objects from all datasets in the bucket.
    keep_last : int
        That last number of runs that should be kept. E.g. a value of 4 will kepp the last 4 runs and remove all prior runs.
    """
    s3 = s3_connection(conn_config)

    if isinstance(dataset_ids, str):
        dataset_ids = [dataset_ids]

    prefix = key_patterns['results'].split('{dataset_id}')[0]

    obj_list = list_parse_s3(s3, bucket, prefix)
    obj_list1 = obj_list[obj_list.KeyDate.notnull()].copy()
    key_split = obj_list1['Key'].str.split('/')
    obj_list1['dataset_id'] = key_split.apply(lambda x: x[2])
    obj_list1['station_id'] = key_split.apply(lambda x: x[3])

    if isinstance(dataset_ids, list):
        obj_list1 = obj_list1[obj_list1['dataset_id'].isin(dataset_ids)].copy()

    ## Get the keys to the objects that should be removed
    obj_list2 = obj_list1.groupby(['dataset_id', 'station_id'])

    rem_keys = []
    for i, row in obj_list2:
        rem1 = row.sort_values('KeyDate', ascending=False).iloc[keep_last:]
        rem_keys.extend(rem1['Key'].tolist())

    if len(rem_keys) > 0:
        ## Split them into 1000 key chunks
        rem_keys_chunks = np.array_split(rem_keys, int(np.ceil(len(rem_keys)/1000)))

        ## Run through and delete the objects...
        for keys in rem_keys_chunks:
            del_list = [{'Key': k} for k in keys]
            resp = s3.delete_objects(Bucket=bucket, Delete={'Objects': del_list})

    print(str(len(rem_keys)) + ' objects removed')

    return rem_keys
















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
