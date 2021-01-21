"""

"""
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
from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats
from geojson import Point
import urllib3
from multiprocessing.pool import ThreadPool
from tethysts.utils import key_patterns

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


def get_last_date(s3_df, default_date='1900-01-01', date_type='date', local_tz=None):
    """

    """
    if not s3_df.empty:
        last_run_date = s3_df['KeyDate'].max().tz_convert(local_tz).tz_localize(None)
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


def station_data_integrety_checks(data, attrs=None, encoding=None):
    """

    """
    # Station data
    essentials = {'lat': (float, np.float), 'lon': (float, np.float), 'station_id': str, 'altitude': (int, float, np.float, np.int)}

    for c in essentials:
        if not c in data.keys():
            raise ValueError('The station_data DataFrame must contain the field: ' + str(c))

        if not isinstance(data[c], essentials[c]):
            raise ValueError('The station_data DataFrame field must have the data type(s): ' + str(essentials[c]))

    return data


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
    station_data1 = station_data_integrety_checks(station_data)

    ## Assign Attributes

    if isinstance(station_attrs, dict):
        attrs1 = copy.deepcopy(station_attrs)
    else:
        attrs1 = {}

    attrs1.update({'station_id': {'cf_role': "timeseries_id", 'virtual_station': virtual_station}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'altitude': {'standard_name': 'surface_altitude', 'long_name': 'height above the geoid to the lower boundary of the atmosphere', 'units': 'm'}})
    if 'name' in station_data.keys():
        attrs1.update({'name': {'long_name': 'station name'}})
    if 'ref' in station_data.keys():
        attrs1.update({'ref': {'long_name': 'station reference id given by the owner'}})

    ts_cols = list(results_data.columns)

    attrs1.update(results_attrs)
    if 'cf_standard_name' in attrs1[param_name]:
        attrs1[param_name]['standard_name'] = attrs1[param_name].pop('cf_standard_name')
    attrs1.update({'height': {'standard_name': 'height', 'long_name': 'vertical distance above the surface', 'units': 'm', 'positive': 'up'}, 'time': {'standard_name': 'time', 'long_name': 'start_time'}})
    if 'modified_date' in ts_cols:
        attrs1.update({'modified_date': {'long_name': 'last modified date'}})

    if isinstance(ancillary_variables, list):
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
    for k, v in station_data.items():
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


def email_msg(sender_address, sender_password, receiver_address, subject, body):
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

    Returns
    -------
    None

    """
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"

    msg_base = """Subject: {subject}\n
    {body}

    hostname: {host}
    IP address: {ip}"""

    ip = requests.get('https://api.ipify.org').text

    msg = msg_base.format(subject=subject, body=body, host=socket.getfqdn(), ip=ip)

    # Create a secure SSL context
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_address, sender_password)
        server.sendmail(sender_address, receiver_address, msg)


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
        ### Validate model
        ds_m = DatasetBase(**ds)

        base_ds = {k: ds[k] for k in base_ds_fields}
        base_ds_b = orjson.dumps(base_ds)
        ds_id = blake2b(base_ds_b, digest_size=12).hexdigest()

        ds['dataset_id'] = ds_id

    return dss


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


def ht_stats(df, parameter, precision):
    """

    """
    min1 = round(float(df[parameter].min()), precision)
    max1 = round(float(df[parameter].max()), precision)
    from_date = df['time'].min()
    to_date = df['time'].max()
    count = int(df['time'].count())

    stats1 = Stats(min=min1, max=max1, count=count, from_date=from_date, to_date=to_date)

    return stats1


def process_object_keys(s3, bucket, prefix):
    """

    """
    keys1 = list_parse_s3(s3, bucket, prefix)
    keys2 = keys1[keys1.Key.str.contains('results.nc.zst')]

    infos1 = [S3ObjectKey(key=row['Key'], bucket=bucket, content_length=row['Size'], etag=row['ETag'], run_date=row['KeyDate'], modified_date=row['LastModified']) for i, row in keys2.iterrows()]

    return infos1


def process_real_station(dataset_id, coords, df, parameter, precision, s3, bucket, geo_type='Point', ref=None, name=None, osm_id=None, altitude=None, properties=None, mod_date=None):
    """

    """
    if mod_date is None:
        mod_date = pd.Timestamp.today(tz='utc').round('s').tz_localize(None)

    geo1 = create_geometry(coords, geo_type='Point')
    station_id = assign_station_id(geo1)

    stats1 = ht_stats(df, parameter, precision)

    prefix = key_patterns['results'].split('{run_date}')[0].format(dataset_id=dataset_id, station_id=station_id)

    object_infos1 = process_object_keys(s3, bucket, prefix)

    station_m = Station(dataset_id=dataset_id, station_id=station_id, virtual_station=False, geometry=geo1, stats=stats1, results_object_key=object_infos1, ref=ref, name=name, osm_id=osm_id, altitude=altitude, properties=properties, modified_date=mod_date)

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


def compare_datasets_from_s3(s3, bucket, new_data, add_old=False):
    """

    """
    ## Determine the parameter, station_id, and dataset_id
    vars1 = list(new_data.variables)
    dataset_id = [new_data[v].attrs['dataset_id'] for v in vars1 if 'dataset_id' in new_data[v].attrs][0]
    station_id = str(new_data['station_id'].values)

    key_dict = {'dataset_id': dataset_id, 'station_id': station_id}

    ## Get list of keys
    prefix_key = key_patterns['results'].split('{run_date}')[0].format(**key_dict)
    all_keys = list_parse_s3(s3, bucket, prefix_key)
    last_key1 = all_keys[all_keys['KeyDate'] == all_keys['KeyDate'].max()]

    ## Get previous data and compare
    if not last_key1.empty:
        last_key = last_key1.iloc[0]['Key']
        obj1 = s3.get_object(Bucket=bucket, Key=last_key)
        b1 = obj1['Body'].read()
        p_old_one = read_pkl_zstd(b1, False)
        xr_old_one = xr.open_dataset(p_old_one)
        xr_old_one['time'] = xr_old_one['time'].dt.round('s')

        up1 = compare_xrs(xr_old_one, new_data, add_old=add_old)
    else:
        print('No prior data found in S3. All data will be returned.')
        up1 = new_data.copy()

    return up1





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
