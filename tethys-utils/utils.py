"""

"""
import os
import io
import numpy as np
import zstandard as zstd
from pdsql import mssql
import pickle
import pandas as pd
from pyproj import Proj, CRS, Transformer


#####################################################
### Functions


def list_parse_s3(s3_client, bucket, prefix, start_after='', delimiter='', continuation_token=''):
    """

    """

    js = []
    while True:
        js1 = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, StartAfter=start_after, Delimiter=delimiter, ContinuationToken=continuation_token)

        if 'Contents' in js1:
            js.extend(js1['Contents'])
            if 'NextContinuationToken' in js1:
                continuation_token = js1['NextContinuationToken'].replace('%3B', ';')
            else:
                break
        else:
            break

    if 'Contents' in js1:
        f_df1 = pd.DataFrame(js).drop('StorageClass', axis=1)
        f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.str.findall('\d\d\d\d\d\d\d\dT\d\d\d\d\d\dZ').apply(lambda x: x[0]), utc=True, errors='coerce')
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


def write_pkl_zstd(obj, file_path=None, compress_level=1):
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
        p_obj = pickle.dumps(obj, protocol=5)

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


def get_hydstra_sites(server, database, username=None, password=None):
    """

    """
    hy_sites = mssql.rd_sql(server, database, 'SITE', ['STATION', 'STNAME', 'EASTING', 'NORTHING', 'OWNER', 'ORGCODE', 'DATEMOD', 'TIMEMOD'], username=username, password=password)
    hy_sites.rename(columns={'STATION': 'site', 'STNAME': 'site_name', 'EASTING': 'NZTMX', 'NORTHING': 'NZTMY', 'OWNER': 'owner', 'ORGCODE': 'orgcode', 'DATEMOD': 'mod_date', 'TIMEMOD': 'mod_time'}, inplace=True)
    hy_sites = hy_sites[hy_sites.site.str.contains('\d+')].copy()

    hy_sites.site = hy_sites.site.str.strip()
    hy_sites.site_name = hy_sites.site_name.str.strip()

    # fix the stupid combo of mod date and time...
    hy_sites['mod_time'] = pd.to_datetime(hy_sites.mod_time + 1, format='%H%M', errors='coerce')
    hy_sites.loc[hy_sites['mod_time'].isnull(), 'mod_time'] = pd.Timestamp('1900-01-01 00:00:00')
    hy_sites['mod_time'] = pd.to_timedelta(hy_sites['mod_time'].dt.time.astype(str), unit='h')
    hy_sites['mod_date'] = pd.to_datetime(hy_sites['mod_date'].dt.date) + hy_sites['mod_time']
    hy_sites.drop('mod_time', axis=1, inplace=True)

    # Remove nonsense sites - Sites with locations in New Zealand
    hy_sites2 = hy_sites[(hy_sites.NZTMX > 1000000) & (hy_sites.NZTMY > 5000000)].copy()

    # Filter out only ECan sites
    hy_sites2.owner = hy_sites2.owner.str.lower().str.strip()
    hy_sites2.orgcode = hy_sites2.orgcode.str.lower().str.strip()

    niwa_sites_bool = (hy_sites2.owner == 'niwa') | (hy_sites2.orgcode == 'n')
    ecs_sites_bool = (hy_sites2.orgcode == 'ecs') | (hy_sites2.owner == 'ecs')
    bor_sites_bool = (hy_sites2.orgcode == 'b')
    ccc_sites_bool = (hy_sites2.orgcode == 'c') | (hy_sites2.owner == 'ccc')
    private_sites_bool = (hy_sites2.orgcode == 'p')

    hy_sites3 = hy_sites2[~(niwa_sites_bool | ecs_sites_bool | bor_sites_bool | ccc_sites_bool | private_sites_bool)].drop(['owner', 'orgcode'], axis=1).copy()

    from_crs = Proj(CRS.from_user_input(2193))
    to_crs = Proj(CRS.from_user_input(4326))

    trans1 = Transformer.from_proj(from_crs, to_crs)
    points1 = trans1.transform(*hy_sites3[['NZTMY', 'NZTMX']].values.T)

    hy_sites3['lon'] = points1[1]
    hy_sites3['lat'] = points1[0]

    return hy_sites3


def get_well_data(server, username, password):
    """

    """
    database = 'Wells'
    well_details_table = 'well_details'
    well_screen_table = 'screen_details'

    well_details_cols = ['well_no', 'nztmx', 'nztmy', 'ground_rl', 'depth', 'diameter', 'screens']
    well_screen_cols = ['well_no', 'top_screen', 'bottom_screen']

    well_d1 = mssql.rd_sql(server, database, well_details_table, well_details_cols, username=username, password=password)
    well_s1 = mssql.rd_sql(server, database, well_screen_table, well_screen_cols, username=username, password=password)

    well_d1['well_no'] = well_d1['well_no'].str.upper().str.strip()
    well_s1['well_no'] = well_s1['well_no'].str.upper().str.strip()

    well_s_grp = well_s1.groupby('well_no')
    well_s_max = well_s_grp['bottom_screen'].max()
    well_s_min = well_s_grp['top_screen'].min()
    well_s2 = pd.concat([well_s_min, well_s_max], axis=1).reset_index()

    well_d2 = pd.merge(well_d1, well_s2, on='well_no', how='left')

    from_crs = Proj(CRS.from_user_input(2193))
    to_crs = Proj(CRS.from_user_input(4326))

    trans1 = Transformer.from_proj(from_crs, to_crs)
    points1 = trans1.transform(*well_d2[['nztmy', 'nztmx']].values.T)

    well_d2['lon'] = np.round(points1[1], 5)
    well_d2['lat'] = np.round(points1[0], 5)

    well_d2.drop(['nztmx', 'nztmy'], axis=1, inplace=True)

    return well_d2.set_index('well_no')


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

    ## Process DataFrame
    site_cols = list(df.columns[~(df.columns.isin(data_cols) | (df.columns == 'time'))])

    print('These are the site columns: ' + ', '.join(site_cols))

    attrs1 = copy.deepcopy(attrs)
    attrs1.update({'station_id': {'cf_role': "timeseries_id", 'virtual_station': False}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'time': {'standard_name': 'time', 'long_name': 'start_time'}})

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
                ds2[e] = ds2[e].round(precision).values
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
