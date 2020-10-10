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

####################################################
### time series types for netcdf

ts_key_pattern = {'H23': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}/{site}.H23.nc',
                  'H25': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}.H25.nc'}

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
            f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.str.findall('\d\d\d\d\d\d\d\dT\d\d\d\d\d\dZ').apply(lambda x: x[0]), utc=True, errors='coerce')
        except:
            print('No dates to parse in Keys')
            f_df1['KeyDate'] = None
        f_df1['ETag'] = f_df1['ETag'].str.replace('"', '')
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


def df_to_xarray(df, nc_type, param_name, attrs, encoding, run_date_key, ancillary_variables=None, compression=False, compress_level=1):
    """

    """
    ## Integrity Checks
    data_cols = []
    if isinstance(ancillary_variables, list):
        for av in ancillary_variables:
            if not av in df:
                raise ValueError('The DataFrame must contain every value in the ancillary_variables list')
            else:
                data_cols.extend([av])

    data_cols.extend([param_name])

    essential_list = [param_name, 'time', 'lat', 'lon', 'station_id']
    no_attrs_list = ['station_name']

    df_cols = list(df.columns)

    for c in essential_list:
        if not c in df_cols:
            raise ValueError('The DataFrame must contain the column: ' + str(c))

    if isinstance(attrs, dict):
        attrs_keys = list(attrs.keys())
        for col in df_cols:
            if not col in no_attrs_list:
                if not col in essential_list:
                    if not col in attrs_keys:
                        raise ValueError('Not all columns are in the attrs dict')
    else:
        raise TypeError('attrs must be a dict')

    if not param_name in encoding:
        raise ValueError(param_name + ' must be in the encoding dict')

    # Make sure station_id data type is a str
    if 'station_id' in df.columns:
        if np.issubdtype(df['station_id'].dtype, np.number):
            print('station_id is a ' + df.station_id.dtype.name + '. It will be converted to a string.' )
            df['station_id'] = df['station_id'].astype(int).astype(str)

    ## Process DataFrame
    site_cols = list(df.columns[~(df.columns.isin(data_cols) | (df.columns == 'time'))])

    print('These are the site columns: ' + ', '.join(site_cols))

    attrs1 = copy.deepcopy(attrs)
    attrs1.update({'station_id': {'cf_role': "timeseries_id", 'virtual_station': False}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'time': {'standard_name': 'time', 'long_name': 'start_time'}})
    if isinstance(ancillary_variables, list):
        attrs1[param_name].update({'ancillary_variables': ' '.join(ancillary_variables)})

    encoding1 = copy.deepcopy(encoding)
    encoding1.update({'time': {'_FillValue': -99999999,'units': "days since 1970-01-01 00:00:00"}, 'lon': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.00001}, 'lat': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.00001}})

    # Convert to specific CF convention data structure
    if nc_type == 'H25':
        ds_cols = data_cols.copy()
        ds_cols.extend(['time'])
        ds1 = df.set_index('station_id')[ds_cols].to_xarray().rename({'station_id': 'stationIndex'})
        attrs1.update({'stationIndex': {'instance_dimension': 'station_id', 'long_name': 'The index for the ragged array'}})

        site_df = df[site_cols].drop_duplicates('station_id').set_index('station_id')
        site_df[['lon', 'lat']] = site_df[['lon', 'lat']].round(5)
        site_ds1 = site_df.to_xarray()

        ds2 = xr.merge([ds1, site_ds1])

    elif nc_type == 'H23':
        ds1 = df.set_index('time')[data_cols].to_xarray()
        site_df = df[site_cols].drop_duplicates('station_id').iloc[0]
        for e, s in site_df.iteritems():
            ds1[e] = s

        ds2 = ds1

    else:
        raise ValueError('nc_type must be either H23 or H25')

    ## Add attributes and encodings
    for e, val in encoding1.items():
        if e in ds2:
            if ('dtype' in val) and (not 'scale_factor' in val):
                if 'int' in val['dtype']:
                    ds2[e] = ds2[e].astype(val['dtype'])
            if 'scale_factor' in val:
                precision = int(np.abs(np.log10(val['scale_factor'])))
                ds2[e] = ds2[e].round(precision)
            ds2[e].encoding = val

    for a, val in attrs1.items():
        if a in ds2:
            ds2[a].attrs = val

    ds_mapping = attrs[param_name]
    title_str = '{agg_stat} {parameter} in {units} of the {feature} by a {method} owned by {owner}'.format(agg_stat=ds_mapping['aggregation_statistic'], parameter=ds_mapping['parameter'], units=ds_mapping['units'], feature=ds_mapping['feature'], method=ds_mapping['method'], owner=ds_mapping['owner'])

    ds2.attrs = {'featureType': 'timeSeries', 'title': title_str, 'institution': ds_mapping['owner'], 'license': ds_mapping['license'], 'source': ds_mapping['method'], 'history': run_date_key + ': Generated'}

    ## Test conversion to netcdf
    p_ts1 = ds2.to_netcdf()

    ## Compress if requested
    if compression:
        cctx = zstd.ZstdCompressor(level=compress_level)
        c_obj = cctx.compress(p_ts1)

        return c_obj
    else:
        return ds2


def grp_ts_agg(df, grp_col, ts_col, freq_code, discrete=False, **kwargs):
    """
    Simple function to aggregate time series with dataframes with a single column of sites and a column of times.

    Parameters
    ----------
    df : DataFrame
        Dataframe with a datetime column.
    grp_col : str or list of str
        Column name that contains the sites.
    ts_col : str
        The column name of the datetime column.
    freq_code : str
        The pandas frequency code for the aggregation (e.g. 'M', 'A-JUN').
    discrete : bool
        Is the data discrete? Will use proper resampling using linear interpolation.

    Returns
    -------
    Pandas resample object
    """

    df1 = df.copy()
    if type(df[ts_col].iloc[0]) is pd.Timestamp:
        df1.set_index(ts_col, inplace=True)
        if isinstance(grp_col, str):
            grp_col = [grp_col]
        else:
            grp_col = grp_col[:]
        if discrete:
            val_cols = [c for c in df1.columns if c not in grp_col]
            df1[val_cols] = (df1[val_cols] + df1[val_cols].shift(-1))/2
        grp_col.extend([pd.Grouper(freq=freq_code, **kwargs)])
        df_grp = df1.groupby(grp_col)
        return (df_grp)
    else:
        print('Make one column a timeseries!')


def compare_dfs(old_df, new_df, on):
    """
    Function to compare two DataFrames with nans and return a dict with rows that have changed (diff), rows that exist in new_df but not in old_df (new), and rows  that exist in old_df but not in new_df (remove).
    Both DataFrame must have the same columns.

    Parameters
    ----------
    old_df : DataFrame
        The old DataFrame.
    new_df : DataFrame
        The new DataFrame.
    on : str or list of str
        The primary key(s) to index/merge the two DataFrames.

    Returns
    -------
    dict of DataFrames
        As described above, keys of 'diff', 'new', and 'remove'.
    """
    if ~np.in1d(old_df.columns, new_df.columns).any():
        raise ValueError('Both DataFrames must have the same columns')

    val_cols = [c for c in old_df.columns if not c in on]
    all_cols = old_df.columns.tolist()

    comp1 = pd.merge(old_df, new_df, on=on, how='outer', indicator=True, suffixes=('_x', ''))

    rem1 = comp1.loc[comp1._merge == 'left_only', on].copy()
    add1 = comp1.loc[comp1._merge == 'right_only', all_cols].copy()
    comp2 = comp1[comp1._merge == 'both'].drop('_merge', axis=1).copy()
#    comp2[comp2.isnull()] = np.nan

    old_cols = on.copy()
    old_cols_map = {c: c[:-2] for c in comp2 if '_x' in c}
    old_cols.extend(old_cols_map.keys())
    old_set = comp2[old_cols].copy()
    old_set.rename(columns=old_cols_map, inplace=True)
    new_set = comp2[all_cols].copy()

    comp_list = []
    for c in val_cols:
        isnull1 = new_set[c].isnull()
        if isnull1.any():
            new_set.loc[new_set[c].isnull(), c] = np.nan
        if old_set[c].dtype.name == 'float64':
            c1 = ~np.isclose(old_set[c], new_set[c])
        elif old_set[c].dtype.name == 'object':
            new_set[c] = new_set[c].astype(str)
            c1 = old_set[c].astype(str) != new_set[c]
        elif old_set[c].dtype.name == 'geometry':
            old1 = old_set[c].apply(lambda x: hash(x.wkt))
            new1 = new_set[c].apply(lambda x: hash(x.wkt))
            c1 = old1 != new1
        else:
            c1 = old_set[c] != new_set[c]
        notnan1 = old_set[c].notnull() | new_set[c].notnull()
        c2 = c1 & notnan1
        comp_list.append(c2)
    comp_index = pd.concat(comp_list, axis=1).any(1)
    diff_set = new_set[comp_index].copy()

    dict1 = {'diff': diff_set, 'new': add1, 'remove': rem1}

    return dict1


def discrete_resample(df, pd_res_code, **kwargs):
    """
    Function to properly set up a resampling class for discrete data. This assumes a linear interpolation between data points.

    Parameters
    ----------
    df: DataFrame
        DataFrame with a time index.
    pd_res_code: str
        Pandas resampling code. e.g. 'D'.
    **kwargs
        Any keyword args passed to Pandas resample.


    Returns
    -------
    Pandas resampling object
    """
    df1 = (df + df.shift(-1))/2
    out1 = df1.resample(pd_res_code, **kwargs)
    return out1
