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
from pandas.core.groupby import SeriesGroupBy, GroupBy
import orjson
from time import sleep
import traceback
from hashlib import blake2b
from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats
from geojson import Point
import urllib3

####################################################
### Misc reference objects

# nc_ts_key_pattern = {'H23': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}/{station}.H23.nc',
                  # 'H25': 'time_series/{owner}/{feature}/{parameter}/{method}/{processing_code}/{aggregation_statistic}/{frequency_interval}/{utc_offset}/{date}.H25.nc'}

nc_ts_key_pattern = {
                    'H23': 'tethys/diff/{dataset_id}/{date}/{station_id}.H23.nc.zst',
                    'H25': 'tethys/diff/{dataset_id}/{date}.H25.nc.zst'
                    }

key_patterns = {'ts': 'tethys/latest/{dataset_id}/{station_id}/ts_data.nc.zst',
                'dataset': 'tethys/latest/datasets.json',
                'station': 'tethys/latest/{dataset_id}/stations.json.zst'
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
    no_attrs_list = ['ref', 'name', 'modified_date']

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
    station_cols = list(df.columns[~(df.columns.isin(data_cols) | (df.columns == 'time'))])

    print('These are the station columns: ' + ', '.join(station_cols))

    attrs1 = copy.deepcopy(attrs)
    attrs1.update({'station_id': {'cf_role': "timeseries_id", 'virtual_station': False}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'time': {'standard_name': 'time', 'long_name': 'start_time'}})
    if 'name' in df_cols:
        attrs1.update({'name': {'long_name': 'station name'}})
    if 'ref' in df_cols:
        attrs1.update({'ref': {'long_name': 'station reference id given by the owner'}})
    if 'modified_date' in df_cols:
        attrs1.update({'modified_date': {'long_name': 'last modified date'}})

    if isinstance(ancillary_variables, list):
        attrs1[param_name].update({'ancillary_variables': ' '.join(ancillary_variables)})

    encoding1 = copy.deepcopy(encoding)
    encoding1.update({'time': {'_FillValue': -99999999, 'units': "days since 1970-01-01 00:00:00"}, 'lon': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.00001}, 'lat': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.00001}})
    if 'modified_date' in df_cols:
        encoding1.update({'modified_date': {'_FillValue': -99999999, 'units': "days since 1970-01-01 00:00:00"}})

    # Convert to specific CF convention data structure
    if nc_type == 'H25':
        ds_cols = data_cols.copy()
        ds_cols.extend(['time'])
        ds1 = df.set_index('station_id')[ds_cols].to_xarray().rename({'station_id': 'stationIndex'})
        attrs1.update({'stationIndex': {'instance_dimension': 'station_id', 'long_name': 'The index for the ragged array'}})

        station_df = df[station_cols].drop_duplicates('station_id').set_index('station_id')
        station_df[['lon', 'lat']] = station_df[['lon', 'lat']].round(5)
        station_ds1 = station_df.to_xarray()

        ds2 = xr.merge([ds1, station_ds1])

    elif nc_type == 'H23':
        ds1 = df.set_index('time')[data_cols].to_xarray()
        station_df = df[station_cols].drop_duplicates('station_id').iloc[0]
        for e, s in station_df.iteritems():
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
        if old_set[c].dtype.type in (np.float32, np.float64):
            c1 = ~np.isclose(old_set[c], new_set[c], equal_nan=True)
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
    {body}"""

    msg = msg_base.format(subject=subject, body=body)

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


def update_remote_dataset(s3, bucket, datasets, run_date=None):
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

    ds_key = key_patterns['dataset']
    try:
        obj1 = s3.get_object(Bucket=bucket, Key=ds_key)
        rem_ds_body = obj1['Body']
        rem_ds = orjson.loads(rem_ds_body.read())
    except:
        rem_ds = []

    if rem_ds != datasets:
        print('datasets are different, datasets.json will be updated')
        all_ids = set([i['dataset_id'] for i in rem_ds])
        all_ids.update(set([i['dataset_id'] for i in datasets]))

        up_list = copy.deepcopy(rem_ds)
        up_list = []

        for i in all_ids:
            rem1 = [n for n in rem_ds if n['dataset_id'] == i]
            new1 = [n for n in datasets if n['dataset_id'] == i]

            if new1:
                up_list.append(new1[0])
            else:
                up_list.append(rem1[0])

        ds_json1 = orjson.dumps(up_list)
        obj2 = s3.put_object(Bucket=bucket, Key=ds_key, Body=ds_json1, Metadata={'run_date': run_date_key}, ContentType='application/json')
    else:
        print('datasets are the same, datasets.json will not be updated')
        up_list = rem_ds

    return up_list


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
    min1 = df[parameter].min().round(precision)
    max1 = df[parameter].max().round(precision)
    from_date = df['time'].min().tz_localize('utc')
    to_date = df['time'].max().tz_localize('utc')
    count = df['time'].count()

    stats1 = Stats(min=min1, max=max1, count=count, from_date=from_date, to_date=to_date)

    return stats1


def process_object_key(s3, bucket, key):
    """

    """
    meta1 = s3.head_object(Bucket=bucket, Key=key)

    object_info1 = S3ObjectKey(key=key, bucket=bucket, content_length=meta1['ContentLength'], etag=meta1['ETag'].replace('"', ''), run_date=pd.Timestamp(meta1['Metadata']['run_date']), modified_date=meta1['LastModified'])

    return object_info1


def process_real_station(dataset_id, coords, df, parameter, precision, s3, bucket, key, geo_type='Point', ref=None, name=None, osm_id=None, altitude=None, properties=None):
    """

    """
    geo1 = create_geometry(coords, geo_type='Point')
    station_id = assign_station_id(geo1)

    stats1 = ht_stats(df, parameter, precision)

    object_info1 = process_object_key(s3, bucket, key)

    station_m = Station(dataset_id=dataset_id, station_id=station_id, virtual_station=False, geometry=geo1, stats=stats1, time_series_object_key=object_info1, ref=ref, name=name, osm_id=osm_id, altitude=altitude, properties=properties)

    return station_m


def get_remote_station(s3, bucket, dataset_id):
    """

    """
    stn_key = key_patterns['station'].format(dataset_id=dataset_id)
    try:
        obj1 = s3.get_object(Bucket=bucket, Key=stn_key)
        rem_stn_body = obj1['Body']
        rem_stn = orjson.loads(read_pkl_zstd(rem_stn_body.read(), unpickle=False))
    except:
        rem_stn = []

    return rem_stn


def update_remote_stations(s3, bucket, dataset_id, station_list, run_date=None):
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

    rem_stn = get_remote_station(s3, bucket, dataset_id)

    if rem_stn != station_list:
        print('stations are different, stations.json.zst will be updated')
        all_ids = set([i['station_id'] for i in rem_stn])
        all_ids.update(set([i['station_id'] for i in station_list]))

        up_list = copy.deepcopy(rem_stn)
        up_list = []

        for i in all_ids:
            rem1 = [n for n in rem_stn if n['station_id'] == i]
            new1 = [n for n in station_list if n['station_id'] == i]

            if new1:
                up_list.append(new1[0])
            else:
                up_list.append(rem1[0])

        stn_json1 = write_pkl_zstd(orjson.dumps(up_list))
        obj2 = s3.put_object(Bucket=bucket, Key=stn_key, Body=stn_json1, Metadata={'run_date': run_date_key}, ContentType='application/json')
    else:
        print('stations are the same, stations.json.zst will not be updated')
        up_list = rem_stn

    return up_list


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
