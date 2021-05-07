#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  7 09:29:29 2021

@author: mike
"""
import numpy as np
import xarray as xr
import pandas as pd
import os
import glob
from tethys_utils.processing import write_pkl_zstd, process_datasets, prepare_results, assign_station_id, make_run_date_key
from tethys_utils.s3 import process_run_date, update_results_s3
from shapely.geometry import shape, mapping, Point, box
import copy
import rasterio


###########################################
### Parameters


############################################
### Functions


def parse_images(path_str):
    """

    """
    f2 = glob.glob(path_str)
    f3 = {f: os.path.getsize(f) for f in f2}
    max1 = max(f3.values())
    max_f = [f for f in f3 if f3[f] == max1][0]

    return f3, max_f


def _split_grid(arr, x_size, y_size, x_name='x', y_name='y'):
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


def _determine_grid_size(arr, starting_x_size=100, starting_y_size=100, increment=100, min_size=800, max_size=1100, x_name='x', y_name='y'):
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
        block_list = _split_grid(arr, x_size=x_size, y_size=y_size, x_name=x_name, y_name=y_name)
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


def _process_grid_blocks(block_list, band_parameter_dict, time, feature, method, product_code, owner, aggregation_statistic, units, data_license, attribution, encoding, description, frequency_interval='T', utc_offset='0H', x_name='x', y_name='y', run_date=None, height=0):
    """

    """
    ## Checks
    bl = block_list[0]
    bands = np.unique(bl.band)
    bp_bands = np.array(list(band_parameter_dict.keys()))
    bp_bands = list(bp_bands[np.in1d(bp_bands, bands)])

    if bp_bands:
        bp_dict = {k: v for k, v in band_parameter_dict.items() if k in bp_bands}
    else:
        raise ValueError('the band_parameter_dict does not contain any of the bands in the images.')

    timestamp = pd.Timestamp(time)
    run_date_key = make_run_date_key(run_date)

    ## Put the dataset metadata together
    if 'scale_factor' in encoding:
        precision = encoding['scale_factor']
    else:
        precision = 1

    ds_base = {
        'feature': feature,
        'method': method,
        'product_code': product_code,
        'owner': owner,
        'aggregation_statistic': aggregation_statistic,
        'units': units,
        'license': data_license,
        'attribution': attribution,
        'description': description,
        'frequency_interval': frequency_interval,
        'utc_offset': utc_offset,
        'spatial_distribution': 'grid',
        'geometry_type': 'Point',
        'grouping': 'blocks',
        'precision': precision,
        'properties': {'encoding': encoding}
        }

    ds_dict = {}
    for k, v in bp_dict.items():
        copy1 = copy.deepcopy(ds_base)
        copy1['parameter'] = v
        new1 = {k: [copy1]}
        ds_dict.update(new1)

    ### Create dataset_ids
    dataset_list = process_datasets(ds_dict)

    # run_date_dict = process_run_date(processing_code, dataset_list, remote, save_interval_hours=336)
    # max_run_date_key = max(list(run_date_dict.values()))

    ## Determine the resolution of the grid
    res = np.mean(np.diff(np.unique(bl.y)))

    ## Create the data_dict
    data_dict = {d['dataset_id']: [] for d in dataset_list}

    ## Iterate through the blocks and bands
    for band in bp_bands:
        print('band: ' + str(band))
        ds_list = ds_dict[band]
        ds = ds_list[0]

        for block in block_list:
            block1 = block.sel(band=band).drop('band')
            block1.name = ds['parameter']
            block1[x_name] = block1[x_name].round(7)
            block1[y_name] = block1[y_name].round(7)

            ## Process station data
            min_x = (block1[x_name].min() - (res*0.5)).round(7)
            max_x = (block1[x_name].max() + (res*0.5)).round(7)
            min_y = (block1[y_name].min() - (res*0.5)).round(7)
            max_y = (block1[y_name].max() + (res*0.5)).round(7)

            extent = box(min_x, min_y, max_x, max_y)
            extent_geo = mapping(extent)
            stn_id = assign_station_id(extent_geo)

            stn_data = {'geometry': extent_geo,
                        'station_id': stn_id}

            ## Process the results
            block1 = block1.rename({x_name: 'lon', y_name: 'lat'})
            df1 = block1.to_dataframe()
            df1['time'] = timestamp
            df1['height'] = height
            df1.set_index(['height', 'time'], append=True, inplace=True)

            ## Package the results
            prepare_results(data_dict, ds_list, stn_data, df1, run_date_key)

    return data_dict



############################################
### Class


class Raster(object):
    """

    """
    ## Initial import and assignment function
    def __init__(self, path_str, x_name='x', y_name='y'):
        f_dict, max_f = parse_images(path_str)

        # Run checks
        src = rasterio.open(max_f)
        crs = src.crs.to_epsg()

        if crs != 4326:
            raise ValueError('Raster CRS is in epsg: ' + str(crs) + ', but should be 4326')

        src.close()

        # Set attrs
        setattr(self, 'x_name', x_name)
        setattr(self, 'y_name', y_name)
        setattr(self, 'max_image', max_f)
        setattr(self, 'images', f_dict)


    def open_big_one(self):
        """

        """
        xr1 = xr.open_rasterio(self.max_image)

        return xr1


    def determine_grid_size(self, starting_x_size=100, starting_y_size=100, increment=100, min_size=800, max_size=1100):
        """

        """
        xr1 = xr.open_rasterio(self.max_image)
        size_dict = _determine_grid_size(xr1, starting_x_size, starting_y_size, increment, min_size, max_size, self.x_name, self.y_name)

        setattr(self, 'grid_size_dict', size_dict)

        res = xr1.attrs['res'][0]
        setattr(self, 'grid_res', res)

        return size_dict


    def process_save_results(self, remote, x_size, y_size, band_parameter_dict, time, feature, method, product_code, owner, aggregation_statistic, units, data_license, attribution, encoding, description, frequency_interval='T', utc_offset='0H', run_date=None, height=0, threads=30, public_url=None):
        """

        """
        ## Prepare input parameters
        run_date_key = make_run_date_key(run_date)

        ## Iterate through the images
        images = list(self.images.keys())
        images.sort()
        for tif in images:
            print(tif)

            xr1 = xr.open_rasterio(tif)
            block_list = _split_grid(xr1, x_size=x_size, y_size=y_size, x_name=self.x_name, y_name=self.y_name)
            data_dict = _process_grid_blocks(block_list, band_parameter_dict, time, feature, method, product_code, owner, aggregation_statistic, units, data_license, attribution, encoding, description, frequency_interval, utc_offset, self.x_name, self.y_name, run_date, height)
            run_date_dict = {ds: run_date_key for ds in data_dict}
            update_results_s3(6, data_dict, run_date_dict, threads, public_url)

        print('Processing and saving data has been successful!')

















