#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  1 13:31:09 2021

@author: mike
"""
import xarray as xr
import numpy as np
import zstandard as zstd
import pandas as pd
import copy
import orjson
from hashlib import blake2b
from tethys_utils.data_models import Geometry, Dataset, DatasetBase, Station, Stats, StationBase
# from data_models import Geometry, Dataset, DatasetBase, Station, Stats, StationBase
from tethys_utils.misc import make_run_date_key, grp_ts_agg, write_pkl_zstd
# from misc import make_run_date_key, grp_ts_agg, write_pkl_zstd
import geojson
from shapely.geometry import shape, mapping
from shapely import wkb

############################################
### Parameters

base_ds_fields = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

agg_stat_mapping = {'mean': 'mean', 'cumulative': 'sum', 'continuous': None, 'maximum': 'max', 'median': 'median', 'minimum': 'min', 'mode': 'mode', 'sporadic': None, 'standard_deviation': 'std', 'incremental': 'cumsum'}

############################################
### Functions


def results_data_integrety_checks(data, param_name, attrs, encoding, spatial_distribution, ancillary_variables=None):
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

    if spatial_distribution == 'sparse':
        ts_index_list = ['geometry', 'time', 'height']
    elif spatial_distribution == 'grid':
        ts_index_list = ['lon', 'lat', 'time', 'height']
    else:
        raise ValueError('spatial_distribution should be either sparse or grid.')

    ts_essential_list = [param_name]
    ts_no_attrs_list = ['modified_date', 'lat', 'lon', 'geometry']

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


def station_data_integrety_checks(data, spatial_distribution, attrs=None, encoding=None):
    """

    """
    stn_m = StationBase(**data)
    stn_data = orjson.loads(stn_m.json(exclude_none=True))

    if spatial_distribution == 'sparse':
        essential_keys = ['geometry', 'station_id']
    elif spatial_distribution == 'grid':
        essential_keys = ['lon', 'lat', 'station_id']
    else:
        raise ValueError('spatial_distribution should be either sparse or grid.')

    for c in essential_keys:
        if not c in stn_data:
            raise ValueError('The Dict must contain the key: ' + str(c))


    ignore_attr_keys = ['station_id', 'lat', 'lon', 'name', 'altitude', 'ref', 'virtual_station', 'geometry']
    other_keys = [s for s in stn_data if s not in ignore_attr_keys]

    if other_keys:

        if isinstance(attrs, dict):
            for col in other_keys:
                if not col in attrs:
                    raise ValueError(col + ' must be in the attrs dict')
        else:
            raise TypeError('attrs must be a dict')

        if isinstance(encoding, dict):
            for col in other_keys:
                if not col in encoding:
                    raise ValueError(col + ' must be in the encoding dict')
        else:
            raise TypeError('encoding must be a dict')

    return stn_data


def metadata_integrety_checks(results_data, station_data, ds_metadata):
    """

    """
    sd = ds_metadata['spatial_distribution']
    geo_type = ds_metadata['geometry_type']
    grouping = ds_metadata['grouping']

    data_index = list(results_data.index.names)
    if ('geometry' in data_index) and (sd == 'grid'):
        raise ValueError('You have passed geometry as an index, but the spatial_distribution is labeled as grid...something is inconsistent.')
    elif (sd == 'sparse') and (('lon' in data_index) or ('lat' in data_index)):
        raise ValueError('You have passed lon/lat as an index, but the spatial_distribution is labeled as sparse...something is inconsistent.')

    stn_geometry = station_data['geometry']
    stn_geo_type = stn_geometry['type']

    if 'geometry' in data_index:
        data_sd = 'sparse'
        hex_geo1 = results_data.index.get_level_values('geometry').unique()
        if len(hex_geo1) == 1:
            data_grouping = 'none'
        else:
            data_grouping = 'blocks'

        data_geometry = wkb.loads(hex_geo1[0], True)
        data_geo_type = data_geometry.type
    elif ('lon' in data_index) and ('lat' in data_index):
        data_sd = 'grid'
        data_geo_type = 'Point'
        lons = results_data.index.get_level_values('lon').unique()
        lats = results_data.index.get_level_values('lat').unique()
        if (len(lons) == 1) and (len(lats) == 1):
            data_grouping = 'none'
        else:
            data_grouping = 'blocks'
    else:
        raise ValueError('Data is not indexed correctly.')

    if data_grouping == 'blocks':
        if stn_geo_type != 'Polygon':
            raise ValueError('If the data grouping is "blocks", then the station geometry should be a Polygon rectangle of the extent.')
    else:
        if stn_geo_type != data_geo_type:
            raise ValueError('If the data grouping is "none", then the station geometry should be the same as the data geometry.')

    if data_geo_type != geo_type:
        raise ValueError('The data geometry type does not match the geometry type listed in the dataset metadata.')
    if data_grouping != grouping:
        raise ValueError('The data grouping does not match the grouping listed in the dataset metadata.')
    if data_sd != sd:
        raise ValueError('The data spatial distribution does not match the spatial distribution listed in the dataset metadata.')


def data_to_xarray(results_data, station_data, param_name, results_attrs, results_encoding, station_attrs=None, station_encoding=None, virtual_station=False, run_date=None, ancillary_variables=None, compression=False, compress_level=1):
    """
    Converts DataFrames of time series data, station data, and other attributes to an Xarray Dataset. Optionally has Zstandard compression.

    Parameters
    ----------
    results_data : DataFrame
        DataFrame of the core parameter and associated ancillary variable. If the spatial distribution is sparse then the data should be indexed by geometry, time, and height. If the spatial distribution is grid then the data should be indexed by lon, lat, time, and height.
    station_data : dict
        Dictionary of the station data. Should include a station_id which should be a hashed string from blake2b (digest_size=12) of the geojson geometry. The other necessary field is geometry, which is the geometry of the results object is not grouped or a boundary extent as a polygon if the results object is grouped. Data owner specific other fields can include "ref" for the reference id and "name" for the station name.
    param_name : str
        The core parameter name of the column in the results_data DataFrame.
    results_attrs : dict
        A dictionary of the xarray/netcdf attributes of the results_data. Where the keys are the columns and the values are the attributes. Only necessary if additional ancilliary valiables are added to the results_data.
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
    ## dataset metadata
    ds_mapping = results_attrs[param_name]
    param_name = ds_mapping['parameter']
    sd = ds_mapping['spatial_distribution']
    grouping = ds_mapping['grouping']
    geo_type = ds_mapping['geometry_type']

    ## Integrity Checks

    # Time series data
    ts_data1 = results_data_integrety_checks(results_data, param_name, results_attrs, results_encoding, sd, ancillary_variables)

    # Station data
    stn_data = station_data_integrety_checks(station_data, sd, attrs=station_attrs, encoding=station_encoding)

    # Metadata
    metadata_integrety_checks(ts_data1, stn_data, ds_mapping)

    ## geometry from station data
    stn_geometry = stn_data['geometry']
    stn_data.pop('geometry')
    if grouping == 'blocks':
        stn_data['extent'] = shape(stn_geometry).wkb_hex

    ## Station properties
    if 'properties' in stn_data:
        props = stn_data['properties'].copy()
        for k, v in props.items():
            stn_data[k] = v
        stn_data.pop('properties')

    ## Assign Attributes
    attrs1 = {'station_id': {'cf_role': "timeseries_id"}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'altitude': {'standard_name': 'surface_altitude', 'long_name': 'height above the geoid to the lower boundary of the atmosphere', 'units': 'm'}, 'geometry': {'long_name': 'The hexadecimal encoding of the Well-Known Binary (WKB) geometry', 'crs_EPSG': 4326}, 'extent': {'long_name': 'The hexadecimal encoding of the Well-Known Binary (WKB) station extent', 'crs_EPSG': 4326}}

    if 'name' in stn_data.keys():
        attrs1.update({'name': {'long_name': 'station name'}})
    if 'ref' in stn_data.keys():
        attrs1.update({'ref': {'long_name': 'station reference id given by the owner'}})

    if isinstance(station_attrs, dict):
        attrs1.update(copy.deepcopy(station_attrs))

    ts_cols = list(ts_data1.columns)

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
    encoding1 = {'lon': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.0000001}, 'lat': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.0000001}, 'altitude': {'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.001}}

    if isinstance(station_encoding, dict):
        encoding1.update(copy.deepcopy(station_encoding))

    height = pd.to_numeric(ts_data1.reset_index()['height'], downcast='integer')

    if 'int' in height.dtype.name:
        height_enc = {'dtype': height.dtype.name}
    elif 'float' in height.dtype.name:
        height_enc = {'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.001}
    else:
        raise TypeError('height should be either an int or a float')

    encoding1.update(results_encoding)
    encoding1.update({'time': {'_FillValue': -99999999, 'units': "days since 1970-01-01 00:00:00"}, 'height': height_enc})
    if 'modified_date' in ts_cols:
        encoding1.update({'modified_date': {'_FillValue': -99999999, 'units': "days since 1970-01-01 00:00:00"}})

    ## Create the Xarray Dataset
    data_index = list(ts_data1.index.names)
    ds1 = ts_data1.to_xarray()

    ## Assign the stn data to the main Dataset
    if 'extent' in stn_data:
        ds1 = ds1.assign_coords({'extent': [stn_data['extent']]})
        stn_data.pop('extent')

    if 'extent' in ds1:
        stn_index = ('extent')
    else:
        if 'geometry' in data_index:
            stn_index = ('geometry')
        else:
            stn_index = ('lon', 'lat')

    ds1 = ds1.assign_coords({'station_id': (stn_index, [stn_data['station_id']])})
    stn_data.pop('station_id')

    for k, v in stn_data.items():
        if k == 'altitude':
            if 'geometry' in data_index:
                ds1 = ds1.assign({k: (('geometry'), [v])})
            else:
                ds1 = ds1.assign({k: (('lon', 'lat'), [v])})
        else:
            ds1 = ds1.assign({k: (('station_id'), [v])})

    ## Assign the lat and lon if the data are sparse points
    if ('geometry' in data_index) and (stn_geometry['type'] == 'Point'):
        ds1 = ds1.assign({'lon': (('geometry'), [stn_geometry['coordinates'][0]])})
        ds1 = ds1.assign({'lat': (('geometry'), [stn_geometry['coordinates'][1]])})

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

    ## Determine if there are ancillary variables to pull through
    new_attrs = new_xr[parameter].attrs.copy()

    if 'ancillary_variables' in new_attrs:
        av1 = new_attrs['ancillary_variables'].split(' ')
        vars2.extend(av1)

    if not parameter in old_xr:
        raise ValueError(parameter + ' must be in old_xr')

    ## Reduce the dimensions for the comparison for compatibility
    new1_s = new_xr[vars2].squeeze()
    old1_s = old_xr[vars2].squeeze()
    on = list(new1_s.dims)

    if not on == list(old1_s.dims):
        raise ValueError('Dimensions are not the same between the datasets')

    keep_vars = on + vars2

    new_all_vars = list(new1_s.variables)
    new_bad_vars = [v for v in new_all_vars if not v in keep_vars]
    new2_s = new1_s.drop(new_bad_vars)

    old_all_vars = list(old1_s.variables)
    old_bad_vars = [v for v in old_all_vars if not v in keep_vars]
    old2_s = old1_s.drop(old_bad_vars)

    # Fix datetime rounding issues...
    for v in list(old2_s.variables):
        if old2_s[v].dtype.name == 'datetime64[ns]':
            old2_s[v] = old2_s[v].dt.round('s')

    for v in list(new2_s.variables):
        if new2_s[v].dtype.name == 'datetime64[ns]':
            new2_s[v] = new2_s[v].dt.round('s')

    ## Pull out data for comparison
    old_df = old2_s.to_dataframe().reset_index()
    new_df = new2_s.to_dataframe().reset_index()

    ## run comparison
    comp = compare_dfs(old_df, new_df, on, parameter, add_old=add_old)

    if comp.empty:
        print('Nothing has changed. Returning empty DataFrame.')
        return comp

    else:

        ## Repackage into netcdf
        comp2 = comp.set_index(list(on)).sort_index().to_xarray()

        # Fix datetime rounding issues...
        for v in list(comp2.variables):
            if comp2[v].dtype.name == 'datetime64[ns]':
                comp2[v] = comp2[v].dt.round('s')

        for v in vars1:
            if v not in vars2:
                comp2[v] = new_xr[v].copy()
                comp2[v].attrs = new_xr[v].attrs.copy()
                comp2[v].encoding = new_xr[v].encoding.copy()

        new_dims = new_xr[parameter].dims
        dim_dict = dict(comp2.dims)
        data_shape = tuple(dim_dict[d] for d in new_dims)

        for v in vars2:
            comp2 = comp2.assign({v: (new_dims, comp2[v].values.reshape(data_shape))})
            comp2[v].attrs = new_xr[v].attrs.copy()
            comp2[v].encoding = new_xr[v].encoding.copy()

        comp2.attrs = new_xr.attrs.copy()
        comp2.encoding = new_xr.encoding.copy()

        return comp2


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
        base_ds_b = orjson.dumps(base_ds, option=orjson.OPT_SERIALIZE_NUMPY)
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


def create_geometry_df(df, extent=False, altitude=True, precision=7):
    """

    """
    if extent:
        if ('lon' in df) and ('lon' in df):
            min_lon = df['lon'].min()
            max_lon = df['lon'].max()
            min_lat = df['lat'].min()
            max_lat = df['lat'].max()
            geometry = geojson.Polygon([[(min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat), (max_lon, min_lat), (min_lon, min_lat)]], True, precision=precision)
        else:
            raise ValueError('Extent must have lat and lon in the df.')
    else:
        if 'geometry' in df:
            geometry = df['geometry']
        elif ('lon' in df) and ('lon' in df):
            if altitude:
                if 'altitude' in df:
                    coords = df.apply(lambda x: (x.lon, x.lat, x.altitude), axis=1)
                else:
                    coords = df.apply(lambda x: (x.lon, x.lat), axis=1)
            else:
                coords = df.apply(lambda x: (x.lon, x.lat), axis=1)
            geometry = coords.apply(lambda x: geojson.Point(x, True, precision=precision))
        else:
            raise ValueError('Either a dict of geometry or a combo of lat and lon must be in the dataframe.')

    return geometry


def assign_station_id(geometry):
    """

    """
    station_id = blake2b(orjson.dumps(geometry, option=orjson.OPT_SERIALIZE_NUMPY), digest_size=12).hexdigest()

    return station_id


def assign_station_ids_df(stns_df, extent=False, precision=5):
    """

    """
    geometry = create_geometry_df(stns_df, extent, False, precision)

    stn_ids = geometry.apply(lambda x: assign_station_id(x))

    return stn_ids


def get_new_stats(data):
    """

    """
    vars1 = list(data.variables)
    parameter = [v for v in vars1 if 'dataset_id' in data[v].attrs][0]
    precision = int(np.abs(np.log10(data[parameter].attrs['precision'])))
    data1 = data[parameter]

    min1 = round(float(data1.min()), precision)
    max1 = round(float(data1.max()), precision)
    mean1 = round(float(data1.mean()), precision)
    median1 = round(float(data1.median()), precision)
    count1 = int(data1.count())
    from_date = pd.Timestamp(data['time'].min().values).tz_localize(None)
    to_date = pd.Timestamp(data['time'].max().values).tz_localize(None)

    stats1 = Stats(min=min1, max=max1, mean=mean1, median=median1, count=count1, from_date=from_date, to_date=to_date)

    return stats1


# def process_stations_df(stns_df, precision=6):
#     """
#     For processing the initial stations prior to assigning the altitude.
#     """
#     stns1 = stns_df.copy()
#     stns1['coords'] = stns1.apply(lambda x: [x.lon, x.lat], axis=1)

#     stns1['geometry'] = stns1.apply(lambda x: geojson.Point(x.coords, True, precision=precision), axis=1)
#     stns1['station_id'] = stns1.apply(lambda x: assign_station_id(x.geometry), axis=1)

#     stns2 = stns1.drop_duplicates('station_id').drop(['coords', 'lat', 'lon'], axis=1).copy()

#     return stns2


def process_station_base(coords=None, station_id=None, geometry=None, ref=None, name=None, osm_id=None, altitude=None, properties=None, return_dict=True):
    """

    """
    if isinstance(coords, list):
        ## Create geometry and station_id
        geometry = geojson.Point(coords, True, precision=5)
        station_id = assign_station_id(geometry)
    elif (not isinstance(station_id, str)) & (not isinstance(geometry, dict)):
        raise ValueError('coords or station_id must be assigned')

    ## Put into data model
    stn_m = StationBase(station_id=station_id, geometry=geometry, ref=ref, name=name, osm_id=osm_id, altitude=altitude, properties=properties)

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

    stn_fields = list(StationBase.schema()['properties'].keys())

    ## Geometry
    if 'geometry' in dims1:
        if len(data['geometry']) == 1:
            geo1 = mapping(wkb.loads(data['geometry'].values[0], True))
        else:
            geo1 = mapping(wkb.loads(data['extent'].values[0], True))

        stn_fields.remove('geometry')

    lat_lon = ['lon', 'lat']

    stn_vars = [v for v in vars1 if (not v in dims1) and (not v in data_vars) and (not v in lat_lon)]
    stn_data1 = {k: v['data'][0] for k, v in data[stn_vars].to_dict()['data_vars'].items() if k in stn_fields}
    stn_data1.update({'station_id': data['station_id'].values[0], 'geometry': geo1})
    if 'altitude' in stn_data1:
        stn_data1['altitude'] = round(stn_data1['altitude'], 3)
    # if not 'virtual_station' in stn_data1:
    #     stn_data1['virtual_station'] = False

    props = {s: {'data': data[s].to_dict()['data'], 'attrs': data[s].to_dict()['attrs']} for s in stn_vars if s not in stn_fields}
    if props:
        stn_data1['properties'] = props

    ## Check model
    stn_m = StationBase(**stn_data1)

    return stn_m.dict(exclude_none=True)


def process_station_summ(dataset_id, station_id, data, object_infos, mod_date=None):
    """

    """
    if mod_date is None:
        mod_date = pd.Timestamp.today(tz='utc').round('s').tz_localize(None)
    elif isinstance(mod_date, (str, pd.Timestamp)):
        mod_date = pd.Timestamp(mod_date).tz_localize(None)

    ## Generate the info for the recently created data
    # station_id = str(data['station_id'].values)
    stats1 = get_new_stats(data)

    ## Put it all together
    stn_dict2 = get_station_data_from_xr(data)
    stn_dict2.update({'dataset_id': dataset_id, 'stats': stats1, 'results_object_key': object_infos, 'modified_date': mod_date})

    station_m = Station(**stn_dict2)

    stn_dict = orjson.loads(station_m.json(exclude_none=True))

    return stn_dict


def prepare_results(data_dict, dataset_list, station_data, results_data, run_date_key, mod_date=None, sum_closed='right', other_closed='left', discrete=True, station_attrs=None, station_encoding=None):
    """

    """
    if isinstance(mod_date, (str, pd.Timestamp)):
        mod_date = pd.Timestamp(mod_date)
        ancillary_variables = ['modified_date']
    else:
        ancillary_variables = []

    tz_str = 'Etc/GMT{0:+}'

    ## Determine index
    if not isinstance(results_data.index, pd.MultiIndex):
        raise TypeError('The data_df index must be a MultiIndex.')

    data_index = list(results_data.index.names)

    if not 'time' in data_index:
        raise ValueError('time must be in the data_df index.')

    other_index = [i for i in data_index if i != 'time']

    ## Iterate through each dataset
    for ds in dataset_list:
        # print(ds['dataset_id'])

        ds_mapping = copy.deepcopy(ds)
        properties = ds_mapping.pop('properties')
        if 'attrs' in properties:
            attrs = properties['attrs']
        else:
            attrs = {}
        encoding = properties['encoding']

        attrs1 = copy.deepcopy(attrs)
        attrs1.update({ds_mapping['parameter']: ds_mapping})

        encoding1 = copy.deepcopy(encoding)

        ## Pre-Process data
        qual_col = 'quality_code'
        freq_code = ds_mapping['frequency_interval']
        utc_offset = ds_mapping['utc_offset']
        parameter = ds_mapping['parameter']

        main_cols = data_index.copy()
        main_cols.extend([parameter])

        ts_data1 = results_data.reset_index().copy()

        # Convert times to local TZ if necessary
        if (not freq_code in ['T', 'H', '1H']) and (not utc_offset == '0H'):
            t1 = int(utc_offset.split('H')[0])
            tz1 = tz_str.format(-t1)
            ts_data1['time'] = ts_data1['time'].dt.tz_localize('UTC').dt.tz_convert(tz1).dt.tz_localize(None)

        ## Aggregate data if necessary
        # Parameter
        if freq_code == 'T':
            grp1 = ts_data1.groupby(data_index)
            data1 = grp1[parameter].mean()

        else:
            agg_fun = agg_stat_mapping[ds_mapping['aggregation_statistic']]

            if agg_fun == 'sum':
                data1 = grp_ts_agg(ts_data1[main_cols], other_index, 'time', freq_code, agg_fun, closed=sum_closed)
            else:
                data1 = grp_ts_agg(ts_data1[main_cols], other_index, 'time', freq_code, agg_fun, discrete, closed=other_closed)

        # Quality code
        if qual_col in ts_data1.columns:
            ts_data1[qual_col] = pd.to_numeric(ts_data1[qual_col], errors='coerce', downcast='integer')
            if freq_code == 'T':
                qual1 = grp1[qual_col].min()
            else:
                qual_cols = data_index.copy()
                qual_cols.extend([qual_col])
                qual1 = grp_ts_agg(ts_data1[qual_cols], other_index, 'time', freq_code, 'min')
            df3 = pd.concat([data1, qual1], axis=1).reset_index().dropna()

            ancillary_variables.extend([qual_col])
        else:
            df3 = data1.reset_index().copy()

        if 'modified_date' in ancillary_variables:
            df3['modified_date'] = mod_date

        ## Convert to xarray
        df4 = df3.copy()

        # Convert time back to UTC if necessary
        if (not freq_code in ['T', 'H', '1H']) and (not utc_offset == '0H'):
            df4['time'] = df4['time'].dt.tz_localize(tz1).dt.tz_convert('utc').dt.tz_localize(None)

        df4.set_index(data_index, inplace=True)

        new1 = data_to_xarray(df4, station_data, parameter, attrs1, encoding1, station_attrs=station_attrs, station_encoding=station_encoding, run_date=run_date_key, ancillary_variables=ancillary_variables, compression='zstd')

        ## Update the data_dict
        ds_id = ds_mapping['dataset_id']

        data_dict[ds_id].append(new1)


def split_grid(arr, x_size, y_size, x_name='x', y_name='y'):
    """
    Function to split an n-dimensional dataset along the x and y dimensions.

    Parameters
    ----------
    arr : DataArray
        An xarray DataArray with at least x and y dimensions. It can have any number of dimensions, though it probably does not make much sense to have greater than 4 dimensions.
    x_size : int
        The size or length of the smaller grids in the x dimension.
    y_size : int
        The size or length of the smaller grids in the y dimension.
    x_name : str
        The x dimension name.
    y_name : str
        The y dimension name.

    Returns
    -------
    List of DataArrays
        The result contains none of the original attributes.
    """
    ## Get the dimension data
    dims = arr.dims
    x_index = dims.index(x_name)
    y_index = dims.index(y_name)
    data_name = arr.name

    arr_shape = arr.shape

    m = arr_shape[x_index]
    n = arr_shape[y_index]
    dtype = arr.dtype

    ## Build the new regular array to be queried
    y_diff = arr[y_name].diff(y_name, 1).median().values
    x_diff = arr[x_name].diff(x_name, 1).median().values

    bpx = ((m-1)//x_size + 1) # blocks per x
    bpy = ((n-1)//y_size + 1) # blocks per y
    M = x_size * bpx
    N = y_size * bpy

    x_y = list(arr_shape)
    x_y[x_index] = M
    x_y[y_index] = N

    sel1 = tuple(slice(0, s) for s in arr_shape)

    A = np.nan * np.ones(x_y)
    A[sel1] = arr

    # x array
    x_start = arr[x_name][0].values
    x_int = M * x_diff
    x_end = x_start + x_int
    xs = np.arange(x_start, x_end, x_diff)

    # y array
    y_start = arr[y_name][0].values
    y_int = M * y_diff
    y_end = y_start + y_int
    ys = np.arange(y_start, y_end, y_diff)

    # Coords
    coords = []
    new_dims = []
    for d in dims:
        name = d
        if d == x_name:
            c = xs
        elif d == y_name:
            c = ys
        else:
            c = arr[d]
        coords.extend([c])
        new_dims.extend([name])

    # New DataArray
    A1 = xr.DataArray(A, coords=coords, dims=new_dims, name=data_name)

    block_list = []
    previous_x = 0
    for x_block in range(bpy):
        previous_x = x_block * x_size
        previous_y = 0
        for y_block in range(bpx):
            previous_y = y_block * y_size
            x_slice = slice(previous_x, previous_x+x_size)
            y_slice = slice(previous_y, previous_y+y_size)

            sel2 = list(sel1)
            sel2[x_index] = x_slice
            sel2[y_index] = y_slice

            block = A1[tuple(sel2)]

            # remove nans
            block = block.dropna(y_name, 'all')
            block = block.dropna(x_name, 'all')

            ## append
            if block.size:
                block_list.append(block.astype(dtype))

    return block_list


def determine_array_size(arr, starting_x_size=100, starting_y_size=100, increment=100, min_size=800, max_size=1100, x_name='x', y_name='y'):
    """
    Function to determine the appropriate grid size for splitting.

    Parameters
    ----------
    arr : DataArray
        An xarray DataArray with at least x and y dimensions. It can have any number of dimensions, though it probably does not make much sense to have greater than 4 dimensions.
    starting_x_size : int
        The initial size or length of the smaller grids in the x dimension.
    starting_y_size : int
        The initial size or length of the smaller grids in the y dimension.
    increment : int
        The incremental grid size to be added iteratively to the starting sizes.
    min_size : int
        The minimum acceptable object size in KB.
    max_size : int
        The maximum acceptable object size in KB.
    x_name : str
        The x dimension name.
    y_name : str
        The y dimension name.

    Returns
    -------
    dict
        Of the optimised grid size results.
    """
    max_obj_size = 0
    x_size = starting_x_size
    y_size = starting_y_size

    while True:
        block_list = split_grid(arr, x_size=x_size, y_size=y_size, x_name=x_name, y_name=y_name)
        obj_sizes = [len(write_pkl_zstd(nc.to_netcdf())) for nc in block_list]
        max_obj_size = max(obj_sizes)

        if max_obj_size < min_size*1000:
            x_size = x_size + increment
            y_size = y_size + increment
        else:
            break

    if max_obj_size > max_size*1000:
        print('max_object_size:', str(max_obj_size))
        raise ValueError('max object size is greater than the allotted size. Reduce the increment value and start again.')

    obj_dict = {'x_size': x_size, 'y_size': y_size, 'max_obj_size': max_obj_size, 'min_obj_size': min(obj_sizes), 'sum_obj_size': sum(obj_sizes), 'len_obj': len(obj_sizes)}

    return obj_dict

























