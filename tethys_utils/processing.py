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
from tethys_utils.misc import make_run_date_key, grp_ts_agg, write_pkl_zstd

############################################
### Parameters

base_ds_fields = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

agg_stat_mapping = {'mean': 'mean', 'cumulative': 'sum', 'continuous': None, 'maximum': 'max', 'median': 'median', 'minimum': 'min', 'mode': 'mode', 'sporadic': None, 'standard_deviation': 'std', 'incremental': 'cumsum'}

############################################
### Functions


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


def station_data_integrety_checks(data, attrs=None, encoding=None):
    """

    """
    # Station data
    ignore_keys = ['station_id', 'lat', 'lon', 'name', 'altitude', 'ref', 'virtual_station', 'geometry']
    other_keys = [s for s in data if s not in ignore_keys]

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
    ## Station data prep
    stn_m = StationBase(**station_data)
    stn_data = orjson.loads(stn_m.json(exclude_none=True))
    stn_data['lon'] = stn_data['geometry']['coordinates'][0]
    stn_data['lat'] = stn_data['geometry']['coordinates'][1]
    stn_data.pop('geometry')
    if 'properties' in stn_data:
        props = stn_data['properties'].copy()
        for k, v in props.items():
            stn_data[k] = v
        stn_data.pop('properties')

    ## Integrity Checks

    # Time series data
    ts_data1 = results_data_integrety_checks(results_data, param_name, results_attrs, results_encoding, ancillary_variables)

    # Station data
    stn_data = station_data_integrety_checks(stn_data, attrs=station_attrs, encoding=station_encoding)

    ## Assign Attributes
    attrs1 = {'station_id': {'cf_role': "timeseries_id", 'virtual_station': virtual_station}, 'virtual_station': {'long_name': 'Is this station a virtual or modeled station?'}, 'lat': {'standard_name': "latitude", 'units': "degrees_north"}, 'lon': {'standard_name': "longitude", 'units': "degrees_east"}, 'altitude': {'standard_name': 'surface_altitude', 'long_name': 'height above the geoid to the lower boundary of the atmosphere', 'units': 'm'}}

    if 'name' in stn_data.keys():
        attrs1.update({'name': {'long_name': 'station name'}})
    if 'ref' in stn_data.keys():
        attrs1.update({'ref': {'long_name': 'station reference id given by the owner'}})

    if isinstance(station_attrs, dict):
        attrs1.update(copy.deepcopy(station_attrs))

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
    encoding1 = {'lon': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.0000001}, 'lat': {'dtype': 'int32', '_FillValue': -999999, 'scale_factor': 0.0000001}, 'altitude': {'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.001}}

    if isinstance(station_encoding, dict):
        encoding1.update(copy.deepcopy(station_encoding))

    height = pd.to_numeric(results_data.reset_index()['height'], downcast='integer')

    if 'int' in height.dtype.name:
        height_enc = {'dtype': height.dtype.name, '_FillValue': -9999}
        # dtype = height.dtype.name
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

    ## Assign the stn data to the main Dataset
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


def create_geometry(coords, geo_type='Point'):
    """

    """
    # print(type(coords[0]))
    if geo_type == 'Point':
        coords = [np.round(coords[0], 5), np.round(coords[1], 5)]
        # geo1 = Point(coords)
        geo1 = {"coordinates": coords, "type": "Point"}
    else:
        raise ValueError('geo_type not implemented yet')

    # if not geo1.is_valid:
    #     raise ValueError('coordinates are not valid')

    geo2 = Geometry(**geo1).dict()

    return geo2


def assign_station_id(geometry):
    """

    """
    station_id = blake2b(orjson.dumps(geometry, option=orjson.OPT_SERIALIZE_NUMPY), digest_size=12).hexdigest()

    return station_id


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


def process_station_base(coords=None, station_id=None, geometry=None, ref=None, name=None, osm_id=None, altitude=None, properties=None, virtual_station=False, geo_type='Point', return_dict=True):
    """

    """
    if isinstance(coords, list):
        ## Create geometry and station_id
        geometry = create_geometry(coords, geo_type=geo_type)
        station_id = assign_station_id(geometry)
    elif (not isinstance(station_id, str)) & (not isinstance(geometry, dict)):
        raise ValueError('coords or station_id must be assigned')

    ## Put into data model
    stn_m = StationBase(station_id=station_id, geometry=geometry, ref=ref, name=name, osm_id=osm_id, altitude=altitude, properties=properties, virtual_station=virtual_station)

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
    stn_fields.extend(['lon', 'lat'])

    stn_vars = [v for v in vars1 if (not v in dims1) and (not v in data_vars)]
    stn_data1 = {k: v['data'] for k, v in data[stn_vars].to_dict()['data_vars'].items() if k in stn_fields}
    stn_data1['geometry'] = create_geometry([stn_data1['lon'], stn_data1['lat']])
    stn_data1.pop('lon')
    stn_data1.pop('lat')
    stn_data1['altitude'] = round(stn_data1['altitude'], 3)
    if not 'virtual_station' in stn_data1:
        stn_data1['virtual_station'] = False

    props = {s: {'data': data[s].to_dict()['data'], 'attrs': data[s].to_dict()['attrs']} for s in stn_vars if s not in stn_fields}
    stn_data1['properties'] = props

    ## Check model
    stn_m = StationBase(**stn_data1)

    return stn_data1


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

    return station_m


def prepare_results(data_dict, dataset_list, station_dict, data_df, run_date_key, mod_date=None, sum_closed='right', other_closed='left', discrete=True, station_attrs=None, station_encoding=None):
    """

    """
    if isinstance(mod_date, (str, pd.Timestamp)):
        mod_date = pd.Timestamp(mod_date)
        ancillary_variables = ['modified_date']
    else:
        ancillary_variables = []

    tz_str = 'Etc/GMT{0:+}'

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

        ts_data1 = data_df.copy()

        # Convert times to local TZ if necessary
        if (not freq_code in ['T', 'H', '1H']) and (not utc_offset == '0H'):
            t1 = int(utc_offset.split('H')[0])
            tz1 = tz_str.format(-t1)
            ts_data1['time'] = ts_data1['time'].dt.tz_localize('UTC').dt.tz_convert(tz1).dt.tz_localize(None)

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

        # Convert time back to UTC if necessary
        if (not freq_code in ['T', 'H', '1H']) and (not utc_offset == '0H'):
            df4['time'] = df4['time'].dt.tz_localize(tz1).dt.tz_convert('utc').dt.tz_localize(None)

        df4.set_index(['time', 'height'], inplace=True)

        new1 = data_to_xarray(df4, station_dict, parameter, attrs1, encoding1, station_attrs=station_attrs, station_encoding=station_encoding, run_date=run_date_key, ancillary_variables=ancillary_variables, compression='zstd')

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

    obj_dict = {'x_size': x_size, 'y_size': y_size, 'max_obj_size': max_obj_size, 'min_obj_size': min(obj_sizes)}

    return obj_dict

























