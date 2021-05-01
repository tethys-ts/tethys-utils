#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  1 13:30:43 2021

@author: mike
"""
import numpy as np
import zstandard as zstd
import pandas as pd
import xarray as xr
import orjson
from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats, StationBase
from multiprocessing.pool import ThreadPool, Pool
from tethysts.utils import key_patterns, get_object_s3, s3_connection, read_json_zstd, read_pkl_zstd
from tethys_utils.misc import make_run_date_key, write_json_zstd, write_pkl_zstd
from tethys_utils.processing import compare_xrs, process_station_summ
from datetime import date, datetime

############################################
### Parameters



############################################
### Functions


def list_objects_s3(s3_client, bucket, prefix, start_after='', delimiter='', continuation_token='', get_versions=False):
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


def list_object_versions_s3(s3_client, bucket, prefix, next_key='', delimiter=None):
    """
    Wrapper S3 function around the list_object_versions base function with a Pandas DataFrame output.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    next_key : str
        The S3 key to start at.
    delimiter : str or None
        A delimiter is a character you use to group keys.

    Returns
    -------
    DataFrame
    """
    js = []
    while True:
        if isinstance(delimiter, str):
            js1 = s3_client.list_object_versions(Bucket=bucket, Prefix=prefix, KeyMarker=next_key, Delimiter=delimiter)
        else:
            js1 = s3_client.list_object_versions(Bucket=bucket, Prefix=prefix, KeyMarker=next_key)

        if 'Versions' in js1:
            js.extend(js1['Versions'])
            if 'NextKeyMarker' in js1:
                next_key = js1['NextKeyMarker']
            else:
                break
        else:
            break

    if js:
        f_df1 = pd.DataFrame(js)[['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size']].copy()
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


def get_object_keys(s3, bucket, prefix):
    """

    """
    keys1 = list_objects_s3(s3, bucket, prefix)
    keys2 = keys1[keys1.Key.str.contains('results')]

    infos1 = [S3ObjectKey(key=row['Key'], bucket=bucket, content_length=row['Size'], etag=row['ETag'], run_date=row['KeyDate'], modified_date=row['LastModified']) for i, row in keys2.iterrows()]

    return infos1


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


def put_remote_agg_stations(s3, bucket, dataset_id, threads=30):
    """

    """
    # base_stn_key = key_patterns['station']
    agg_stn_key = key_patterns['stations']

    stn_prefix = agg_stn_key.split('stations.json.zst')[0].format(dataset_id=dataset_id)

    list1 = list_objects_s3(s3, bucket, stn_prefix)
    list2 = list1[list1.Key.str.contains('station.json.zst')].copy()

    # stn_list = [{'s3': s3, 'bucket': bucket, 'dataset_id': None, 'stn_key': k} for k in list2.Key]
    stn_list = [[s3, bucket, None, None, k] for k in list2.Key]

    output = ThreadPool(threads).starmap(get_remote_station, stn_list)

    stns_obj = write_json_zstd(output)

    run_date_key = make_run_date_key()
    stns_key = agg_stn_key.format(dataset_id=dataset_id)
    s3.put_object(Body=stns_obj, Bucket=bucket, Key=stns_key, ContentType='application/json', Metadata={'run_date': run_date_key})

    return output


def put_remote_agg_datasets(s3, bucket, threads=30):
    """

    """
    # base_ds_key = key_patterns['dataset']
    agg_ds_key = key_patterns['datasets']

    ds_prefix = agg_ds_key.split('datasets.json.zst')[0]

    list1 = list_objects_s3(s3, bucket, ds_prefix)
    list2 = list1[list1.Key.str.contains('dataset.json.zst')].copy()

    # ds_list = [{'s3': s3, 'bucket': bucket, 'dataset_id': None, 'ds_key': k} for k in list2.Key]
    ds_list = [[s3, bucket, None, k] for k in list2.Key]

    output = ThreadPool(threads).starmap(get_remote_dataset, ds_list)

    dss_obj = write_json_zstd(output)

    run_date_key = make_run_date_key()
    dss_key = agg_ds_key
    s3.put_object(Body=dss_obj, Bucket=bucket, Key=dss_key, ContentType='application/json', Metadata={'run_date': run_date_key})

    return output


def compare_datasets_from_s3(conn_config, bucket, new_data, add_old=False, last_run_date_key=None, public_url=None):
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

    base_key_pattern = key_patterns['results']

    ## Get list of keys

    if isinstance(last_run_date_key, str):
        key_dict.update({'run_date': last_run_date_key})
        last_key = base_key_pattern.format(**key_dict)
    else:
        last_key = None

    ## Get previous data and compare
    if isinstance(public_url, str):
        conn_config = public_url

    if isinstance(last_key, str):
        # last_key = last_key1.iloc[0]['Key']
        p_old_one = get_object_s3(last_key, conn_config, bucket, 'zstd')
        xr_old_one = xr.load_dataset(p_old_one)
        xr_old_one['time'] = xr_old_one['time'].dt.round('s')

        up1 = compare_xrs(xr_old_one, new_data, add_old=add_old)
    else:
        print('No prior data found in S3. All data will be returned.')
        up1 = new_data.copy()

    return up1


def update_results_s3(processing_code, data_dict, run_date_dict, remote, threads=20, public_url=None):
    """
    Parameters
    ----------
    processing_code : int
        The processing code to determine how the input data should be processed.
    data_dict : dict of lists
        A dictionary with the keys as the dataset_ids and teh values as lists of zstd compressed xr.Datasets.
    remote : dict
        Dict of a connection_config and bucket:
        conn_config : dict
            A dictionary of the connection info necessary to establish an S3 connection.
        bucket : str
            The S3 bucket.
    threads : int
        The number of threads to use to process the data.
    public_url : str
        Optional if there is a public URL to the object instead of using the S3 API directly.

    Returns
    -------
    None

    """
    ### Parameters
    conn_config = remote['connection_config']
    bucket = remote['bucket']

    if processing_code in [2, 3, 6]:
        add_old = True
    elif processing_code in [1, 4, 5]:
        add_old = False
    else:
        raise ValueError('processing_code does not exist.')

    # if processing_code in [3]:
    #     read_buffer = True
    # else:
    #     read_buffer = False

    if processing_code in [4, 5]:
        no_comparison = True
    else:
        no_comparison = False

    ### Run update

    s3 = s3_connection(conn_config, threads)

    for ds_id, results in data_dict.items():
        print('--dataset_id: ' + ds_id)

        run_date_key = run_date_dict[ds_id]

        # Create the Key info dict
        prefix = key_patterns['results'].split('{station_id}')[0].format(dataset_id=ds_id)

        keys1 = list_objects_s3(s3, bucket, prefix)
        obj_df1 = keys1[keys1.Key.str.contains('results.nc')].copy()
        # obj_df1['dataset_id'] = obj_df1['Key'].apply(lambda x: x.split('/')[2])
        obj_df1['station_id'] = obj_df1['Key'].apply(lambda x: x.split('/')[3])

        last_date1 = obj_df1.groupby(['station_id'])['KeyDate'].last().reset_index()


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
            mod_date_key = new1.attrs['history'].split(':')[0]

            if no_comparison:
                up1 = new1
            else:
                last_date_key_df = last_date1[last_date1['station_id'] == stn_id]
                if last_date_key_df.empty:
                    last_date_key = None
                else:
                    last_date_key = make_run_date_key(last_date_key_df['KeyDate'].iloc[0])

                up1 = compare_datasets_from_s3(conn_config, bucket, new1, add_old=add_old, read_buffer=False, last_run_date_key=last_date_key, public_url=public_url)

            ## Save results
            if isinstance(up1, xr.Dataset) and (len(up1[parameter].time) > 0):

                # print('Save results')
                key_dict = {'dataset_id': ds_id, 'station_id': stn_id, 'run_date': run_date_key}

                new_key = key_patterns['results'].format(**key_dict)

                cctx = zstd.ZstdCompressor(level=1)
                c_obj = cctx.compress(up1.to_netcdf())

                obj_resp = s3.put_object(Body=c_obj, Bucket=bucket, Key=new_key, ContentType='application/zstd', Metadata={'run_date': mod_date_key})

                ## Process stn data
                # print('Save station data')

                ## Process object key infos
                stn_obj_df1 = obj_df1[obj_df1['station_id'] == stn_id].drop(['LastModified', 'station_id'], axis=1).copy()
                keys = stn_obj_df1['Key'].unique()
                new_key_info = [new_key, obj_resp['ResponseMetadata']['HTTPHeaders']['etag'].replace('"', ''), len(c_obj), pd.Timestamp(run_date_key).tz_localize(None)]

                if new_key in keys:
                    stn_obj_df1[stn_obj_df1['Key'] == new_key] = new_key_info
                else:
                    stn_obj_df1.loc[len(stn_obj_df1)] = new_key_info

                info1 = [S3ObjectKey(key=row['Key'], bucket=bucket, content_length=row['Size'], etag=row['ETag'], run_date=row['KeyDate']) for i, row in stn_obj_df1.iterrows()]

                ## Final station processing and saving
                stn_m = process_station_summ(ds_id, stn_id, up1, info1, mod_date=mod_date_key)

                stn4 = orjson.loads(stn_m.json(exclude_none=True))
                up_stns = put_remote_station(s3, bucket, stn4, run_date=mod_date_key)

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


def delete_result_objects_s3(conn_config, bucket, dataset_ids=None, keep_last=10, threads=50):
    """
    Function to delete Tethys result objects including all object versions.

    Parameters
    ----------
    conn_config : dict
        A dictionary of the connection info necessary to establish an S3 connection.
    bucket : str
        The s3 bucket.
    dataset_ids : list, str, or None
        The specific datasets that should have the results objects removed. None will remove results objects from all datasets in the bucket.
    keep_last : int
        That last number of runs that should be kept. E.g. a value of 4 will kepp the last 4 runs and remove all prior runs.
    threads : int
        The number of concurrent threads to use when aggregating the stations and datasets.

    Returns
    -------
    list of keys deleted
    """
    s3 = s3_connection(conn_config, threads)

    if isinstance(dataset_ids, str):
        dataset_ids = [dataset_ids]

    prefix = key_patterns['results'].split('{dataset_id}')[0]

    obj_list = list_object_versions_s3(s3, bucket, prefix)

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
        for i2, row2 in rem1.iterrows():
            rem_keys.extend([{'Key': row2['Key'], 'VersionId': row2['VersionId']}])

    if len(rem_keys) > 0:
        ## Split them into 1000 key chunks
        rem_keys_chunks = np.array_split(rem_keys, int(np.ceil(len(rem_keys)/1000)))

        ## Run through and delete the objects...
        for keys in rem_keys_chunks:
            resp = s3.delete_objects(Bucket=bucket, Delete={'Objects': keys.tolist(), 'Quiet': True})

    print(str(len(rem_keys)) + ' objects removed')
    print('Updating stations and dataset json files...')

    dataset_list = obj_list1['dataset_id'].unique().tolist()

    for ds in dataset_list:
        ds_stations = put_remote_agg_stations(s3, bucket, ds, threads)

    ### Aggregate all datasets for the bucket
    ds_all = put_remote_agg_datasets(s3, bucket, threads)

    return rem_keys


def process_run_date(processing_code, dataset_list, remote, run_date=None, save_interval_hours=336):
    """
    Function to process the run date keys for all datasets for the extraction. These are specific to each processing_code.

    Parameters
    ----------
    processing_code : int
        The processing code to determine how the input data should be processed.
    dataset_list : list of dict
        The list of datasets, which is the output of the process_datasets function.
    remote : dict
        Dict of a connection_config and bucket:
        conn_config : dict
            A dictionary of the connection info necessary to establish an S3 connection.
        bucket : str
            The S3 bucket.
    run_date : str, datetime, date, pd.Timestamp, or None
        The run_date to use for processing. If None, then the run_date will be generated.
    save_interval_hours : int
        The frequency by which the datasets should have run dates saved as in Tethys. Essentially the question is, how frequently do you want to keep a record of the data changes? This value is in hours and the default is 2 weeks. This does not effect datasets with processing_code 3 as the run_date always stays the same.

    Returns
    -------
    run_date_dict : dict of str
    """
    if isinstance(run_date, (str, datetime, date, pd.Timestamp)):
        run_date1 = pd.Timestamp(run_date)
    else:
        run_date1 = pd.Timestamp.today(tz='utc').tz_localize(None)

    # run_date_key = run_date1.strftime('%Y%m%dT%H%M%SZ')

    s3 = s3_connection(remote['connection_config'])

    run_date_dict = {}
    for ds in dataset_list:
        dataset_id = ds['dataset_id']

        ## Get last result dates
        prefix = key_patterns['results'].split('{station_id}')[0].format(dataset_id=dataset_id)
        obj_list = list_objects_s3(s3, remote['bucket'], prefix)

        if obj_list.empty:
            last_run_date = run_date1
            last_run_date_key = run_date1.strftime('%Y%m%dT%H%M%SZ')
        else:
            last_run_date = obj_list['KeyDate'].max()

            if processing_code in [3]:
                last_run_date_key = last_run_date.strftime('%Y%m%dT%H%M%SZ')
            elif processing_code in [1, 2, 4, 5, 6]:
                if last_run_date < (run_date1 - pd.DateOffset(hours=save_interval_hours) + pd.DateOffset(minutes=5)):
                    last_run_date_key = run_date1.strftime('%Y%m%dT%H%M%SZ')
                else:
                    last_run_date_key = last_run_date.strftime('%Y%m%dT%H%M%SZ')
            else:
                raise ValueError('processing_code does not exist.')

        run_date_dict.update({dataset_id: last_run_date_key})

    return run_date_dict
























































