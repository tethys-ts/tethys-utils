"""

"""
import os
import io
import numpy as np
import zstandard as zstd
import pandas as pd
import copy
import xarray as xr
import orjson
from time import sleep
import traceback
from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats
from tethys_utils.main import nc_ts_key_pattern, key_patterns, assign_ds_ids, update_remote_dataset, create_geometry, assign_station_id, grp_ts_agg, read_pkl_zstd, compare_dfs, df_to_xarray, process_real_station, update_remote_stations, email_msg, s3_connection
import urllib3


##########################################################
### Functions for Hilltop data extraction


def convert_site_names(names, rem_m=True):
    """
    Function to convert water usage site names.
    """

    names1 = names.str.replace('[:\.]', '/')
#    names1.loc[names1 == 'L35183/580-M1'] = 'L35/183/580-M1' What to do with this one?
#    names1.loc[names1 == 'L370557-M1'] = 'L37/0557-M1'
#    names1.loc[names1 == 'L370557-M72'] = 'L37/0557-M72'
#    names1.loc[names1 == 'BENNETT K38/0190-M1'] = 'K38/0190-M1'
    names1 = names1.str.upper()
    if rem_m:
        list_names1 = names1.str.findall('[A-Z]+\d*/\d+')
        names_len_bool = list_names1.apply(lambda x: len(x)) == 1
        names2 = names1.copy()
        names2[names_len_bool] = list_names1[names_len_bool].apply(lambda x: x[0])
        names2[~names_len_bool] = np.nan
    else:
        list_names1 = names1.str.findall('[A-Z]+\d*/\d+\s*-\s*M\d*')
        names_len_bool = list_names1.apply(lambda x: len(x)) == 1
        names2 = names1.copy()
        names2[names_len_bool] = list_names1[names_len_bool].apply(lambda x: x[0])
        names2[~names_len_bool] = np.nan

    return names2


def get_hilltop_water_use_data(param, ts_local_tz, station_mtype_corrections=None):
    """

    """
    import requests
    from hilltoppy import web_service as ws
    from hilltoppy import util

    try:

        ### Read in parameters

        # mod_local_tz = 'Pacific/Auckland'

        base_url = param['source']['api_endpoint']
        hts = param['source']['hts']

        datasets = param['source']['dataset_mapping']

        encoding_keys = ['scale_factor', 'dtype', '_FillValue']
        base_keys = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

        base_url = param['source']['api_endpoint']
        hts = param['source']['hts']

        nc_type = param['source']['nc_type']
        base_key_pattern = nc_ts_key_pattern[nc_type]
        last_run_key_pattern = key_patterns['ts']

        stn_key_pattern = key_patterns['station']
        # key_pattern = base_key_pattern + '.zst'
        # last_run_key_pattern = 'last_run' + base_key_pattern[11:].split('{date}')[0] + '{station}.H23.nc.zst'

        # data_dir = 'data'

        attrs = {'quality_code': {'standard_name': 'quality_flag', 'long_name': 'NEMS quality code', 'references': 'https://www.lawa.org.nz/media/16580/nems-quality-code-schema-2013-06-1-.pdf'}}

        encoding = {'quality_code': {'dtype': 'int16', '_FillValue': -9999}}

        gauging_measurements = ['Flow [Gauging Results]', 'Stage', 'Area', 'Velocity [Gauging Results]', 'Max Depth', 'Slope', 'Width', 'Hyd Radius', 'Wet. Perimeter', 'Sed. Conc.', 'Temperature', 'Stage Change [Gauging Results]', 'Method', 'Number Verts.', 'Gauge Num.']

        ### Initalize

        run_date = pd.Timestamp.today(tz='utc').round('s')
        run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')

        # if not os.path.exists(data_dir):
        #     os.mkdir(data_dir)

        s3 = s3_connection(param['remote']['connection_config'])

        ### Get remote objects
        # s3_objects1 = list_parse_s3(s3, param['remote']['bucket'], last_run_key_pattern.split('{dataset_id}')[0])

        ### Create dataset_ids, check if datasets.json exist on remote, and if not add it
        for ht_ds, ds_list in datasets.items():
            ds_list2 = assign_ds_ids([ds_list])
            datasets[ht_ds] = ds_list2[0]

        dataset_list = []
        [dataset_list.extend([ds_list]) for ht_ds, ds_list in datasets.items()]

        dataset_list = update_remote_dataset(s3, param['remote']['bucket'], dataset_list, run_date_key)

        ### Pull out stations
        stns1 = ws.site_list(base_url, hts, location='LatLong')
        stns2 = stns1[~((stns1.lat < -47.5) & (stns1.lat > -34) & (stns1.lon > 166) & (stns1.lon < 179))].dropna().copy()
        stns2.rename(columns={'SiteName': 'Site'}, inplace=True)

        stns2['ref'] = convert_site_names(stns2.Site)
        stns2 = stns2.dropna().drop_duplicates('ref').copy()

        stns2['geo'] = stns2.apply(lambda x: create_geometry([x.lon, x.lat]), axis=1)
        stns2['station_id'] = stns2.apply(lambda x: assign_station_id(x.geo), axis=1)

        ### Pull out the mtypes
        print('-Running through station/measurement combos')

        mtypes_list = []
        for s in stns2.Site:
            print(s)
            try:
                meas1 = ws.measurement_list(base_url, hts, s, measurement='Abstraction Volume')
            except:
                print('** station is bad')
            mtypes_list.append(meas1)
        mtypes_df = pd.concat(mtypes_list).reset_index()
        mtypes_df = mtypes_df[mtypes_df.Measurement == 'Abstraction Volume'].copy()
        mtypes_df2 = pd.merge(mtypes_df, stns2, on='Site')

        mtypes_df2['feature'] = 'gw'
        mtypes_df2.loc[mtypes_df2.ref.str.contains('SW/'), 'feature'] = 'sw'

        ## Make corrections to mtypes
        if station_mtype_corrections is not None:
            for i, f in station_mtype_corrections.items():
                mtypes_df2.loc[(mtypes_df.Site == i[0]) & (mtypes_df2.Measurement == i[1]), 'From'] = f

        # save_folder_flag = set()
        # stns_dict = {d['dataset_id']: [] for d in dataset_list}

        for feat, ds in datasets.items():

            mtypes_df3 = mtypes_df2[mtypes_df2.feature == feat].copy()

            ds_mapping = ds.copy()
            properties = ds_mapping['properties']
            ds_mapping.pop('properties')

            attrs1 = {}
            attrs1.update({ds_mapping['parameter']: ds_mapping})

            encoding1 = copy.deepcopy(encoding)
            encoding1.update({ds_mapping['parameter']: properties['encoding']})

            freq_code = ds_mapping['frequency_interval']
            parameter = ds_mapping['parameter']
            precision = int(np.abs(np.log10(encoding1[parameter]['scale_factor'])))

            base_key_dict = {'dataset_id': ds['dataset_id']}

            # base_key_dict = {k: v for k, v in ds_mapping.items() if k in base_keys}

            ## Open the old site list
            old_stns1 = get_remote_station(s3, param['remote']['bucket'], ds['dataset_id'])

            ## Iterate through the sites/mtypes
            for i, row in mtypes_df3.iterrows():
                print(row.Site)

                new_to_date = row.To.round('D') - pd.DateOffset(hours=36)

                stn = [s for s in old_stns1 if s['station_id'] == row.station_id]

                if len(stn) > 0:
                    old_to_date = pd.Timestamp(stn[0]['stats']['to_date']).tz_localize(None)
                else:
                    old_to_date = pd.Timestamp('1900-01-01')

                if new_to_date > old_to_date:

                    old_to_date1 = old_to_date - pd.DateOffset(hours=132) - pd.DateOffset(seconds=1)

                    if old_to_date1 < row.From:
                        old_to_date1 = row.From

                    print('- Extracting data...')
                    timer = 5
                    while timer > 0:

                        try:
                            ts_data = ws.get_data(base_url, hts, row.Site, row.Measurement, from_date=str(old_to_date1), to_date=str(row.To), agg_method='Total', agg_interval='1 day')[1:].reset_index()
                            break
                        except requests.exceptions.ConnectionError as err:
                            print(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            timer = timer - 1
                            sleep(30)
                        except ValueError as err:
                            print(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            break
                        except Exception as err:
                            print(str(err))
                            timer = timer - 1
                            sleep(30)

                    if timer == 0:
                        raise ValueError('The Hilltop request tried too many times...the server is probably down')

                    ## Pre-Process data
                    print('Pre-Process data')
                    ts_data['DateTime'] = ts_data['DateTime'] - pd.DateOffset(days=1)

                    ts_data1 = ts_data.drop(['Site', 'Measurement'], axis=1).rename(columns={ 'Value': parameter, 'DateTime': 'time'}).copy()

                    ts_data1[parameter] = pd.to_numeric(ts_data1[parameter], errors='coerce')
                    ts_data1['time'] = ts_data1['time'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc').dt.tz_localize(None)


                    ## Add to last data
                    key_dict = base_key_dict.copy()
                    key_dict.update({'station_id': row.station_id})

                    last_key = last_run_key_pattern.format(**key_dict)

                    try:
                        obj1 = s3.get_object(Bucket=param['remote']['bucket'], Key=last_key)
                        b1 = obj1['Body'].read()
                        p_old_one = read_pkl_zstd(b1, False)
                        xr_old_one = xr.open_dataset(p_old_one)
                        old_one = xr_old_one.to_dataframe().drop(['lat', 'lon', 'ref', 'station_id'], axis=1).reset_index()
                        old_one['time'] = old_one.time.dt.round('s')
                        old_one[parameter] = old_one[parameter].round(precision)

                    except:
                        print('No prior data found. All data will be saved.')
                        old_one = pd.DataFrame()

                    if not old_one.empty:
                        ts_data1 = pd.concat([old_one, ts_data1]).drop_duplicates(subset='time', keep='last')


                    ## Add in the station data and create the time series object
                    ts_data1['ref'] = row.ref
                    ts_data1['lat'] = row.lat
                    ts_data1['lon'] = row.lon
                    ts_data1['station_id'] = row.station_id

                    p_all1 = df_to_xarray(ts_data1, nc_type, parameter, attrs1, encoding1, run_date_key, None, True)

                    ## Save the data
                    key_dict = base_key_dict.copy()
                    key_dict.update({'station_id': row.station_id})

                    last_key = last_run_key_pattern.format(**key_dict)

                    print('Save last complete data')

                    s3.put_object(Body=p_all1, Bucket=param['remote']['bucket'], Key=last_key, ContentType='application/zstd', Metadata={'run_date': run_date_key})

                    ## Process stn data
                    print('Save station data')
                    stn_m = process_real_station(ds['dataset_id'], [float(row.lon), float(row.lat)], ts_data1, parameter, precision, s3, param['remote']['bucket'], last_key, ref=row['ref'])

                    stn4 = orjson.loads(stn_m.json(exclude_none=True))
                    up_stns = update_remote_stations(s3, param['remote']['bucket'], ds['dataset_id'], [stn4], run_date=run_date)

                else:
                    print('No data to udapte')

        print('--Success!')

    except Exception as err:
        # print(err)
        print(traceback.format_exc())
        email_msg(param['remote']['email']['sender_address'], param['remote']['email']['sender_password'], param['remote']['email']['receiver_address'], 'Failure on tethys-extraction-es-hilltop water-use', traceback.format_exc())












def get_qc_hilltop_data(param, ts_local_tz, station_mtype_corrections=None):
    """

    """
    import requests
    from hilltoppy import web_service as ws

    try:

        ### Read in parameters

        # mod_local_tz = 'Pacific/Auckland'

        base_url = param['source']['api_endpoint']
        hts = param['source']['hts']

        datasets = param['source']['dataset_mapping']

        encoding_keys = ['scale_factor', 'dtype', '_FillValue']
        base_keys = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

        base_url = param['source']['api_endpoint']
        hts = param['source']['hts']

        nc_type = param['source']['nc_type']
        base_key_pattern = nc_ts_key_pattern[nc_type]
        last_run_key_pattern = key_patterns['ts']

        stn_key_pattern = key_patterns['station']
        # key_pattern = base_key_pattern + '.zst'
        # last_run_key_pattern = 'last_run' + base_key_pattern[11:].split('{date}')[0] + '{station}.H23.nc.zst'

        # data_dir = 'data'

        attrs = {'quality_code': {'standard_name': 'quality_flag', 'long_name': 'NEMS quality code', 'references': 'https://www.lawa.org.nz/media/16580/nems-quality-code-schema-2013-06-1-.pdf'}}

        encoding = {'quality_code': {'dtype': 'int16', '_FillValue': -9999}}

        gauging_measurements = ['Flow [Gauging Results]', 'Stage', 'Area', 'Velocity [Gauging Results]', 'Max Depth', 'Slope', 'Width', 'Hyd Radius', 'Wet. Perimeter', 'Sed. Conc.', 'Temperature', 'Stage Change [Gauging Results]', 'Method', 'Number Verts.', 'Gauge Num.']

        ### Initalize

        run_date = pd.Timestamp.today(tz='utc').round('s')
        run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')

        # if not os.path.exists(data_dir):
        #     os.mkdir(data_dir)

        s3 = s3_connection(param['remote']['connection_config'])

        ### Get remote objects
        # s3_objects1 = list_parse_s3(s3, param['remote']['bucket'], last_run_key_pattern.split('{dataset_id}')[0])

        ### Create dataset_ids, check if datasets.json exist on remote, and if not add it
        for ht_ds, ds_list in datasets.items():
            ds_list2 = assign_ds_ids(ds_list)
            datasets[ht_ds] = ds_list2

        dataset_list = []
        [dataset_list.extend(ds_list) for ht_ds, ds_list in datasets.items()]

        dataset_list = update_remote_dataset(s3, param['remote']['bucket'], dataset_list, run_date_key)

        ### Pull out stations
        stns1 = ws.site_list(base_url, hts, location='LatLong')
        stns2 = stns1[~((stns1.lat < -47.5) & (stns1.lat > -34) & (stns1.lon > 166) & (stns1.lon < 179))].dropna().copy()
        stns2.rename(columns={'SiteName': 'ref'}, inplace=True)

        stns2['geo'] = stns2.apply(lambda x: create_geometry([x.lon, x.lat]), axis=1)
        stns2['station_id'] = stns2.apply(lambda x: assign_station_id(x.geo), axis=1)

        print('-Running through station/measurement combos')

        mtypes_list = []
        for s in stns2.ref:
            print(s)
            try:
                meas1 = ws.measurement_list(base_url, hts, s)
            except:
                print('** station is bad')
            mtypes_list.append(meas1)
        mtypes_df = pd.concat(mtypes_list).reset_index()

        ## Make corrections to mtypes
        if station_mtype_corrections is not None:
            for i, f in station_mtype_corrections.items():
                mtypes_df.loc[(mtypes_df.Site == i[0]) & (mtypes_df.Measurement == i[1]), 'From'] = f

        # save_folder_flag = set()
        stns_dict = {d['dataset_id']: [] for d in dataset_list}

        for meas in datasets:

            print(meas)

            mtypes_df2 = mtypes_df[mtypes_df.Measurement == meas].copy()

            if not mtypes_df2.empty:

                ##  Iterate through each stn
                for i, row in mtypes_df2.iterrows():
                    print(row.Site)

                    ## Get the data out
                    print('- Extracting data...')
                    timer = 5
                    while timer > 0:
                        ts_data_list = []

                        try:
                            start_date = row.From
                            while True:
                                sleep(3)
                                to_date = start_date + pd.DateOffset(years=20)
                                if to_date > row.To:
                                    to_date = row.To
                                ts_data_chunk = ws.get_data(base_url, hts, row.Site, row.Measurement, from_date=str(start_date), to_date=str(to_date), quality_codes=True)
                                ts_data_list.append(ts_data_chunk)
                                start_date = to_date
                                if to_date >= row.To:
                                    break

                            break
                        except requests.exceptions.ConnectionError as err:
                            print(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            timer = timer - 1
                            sleep(30)
                        except ValueError as err:
                            print(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            break
                        except Exception as err:
                            print(str(err))
                            timer = timer - 1
                            sleep(30)

                    if timer == 0:
                        raise ValueError('The Hilltop request tried too many times...the server is probably down')

                    ts_data = pd.concat(ts_data_list)

                    ## Iterate through each dataset
                    for ds in datasets[meas]:
                        print(ds)

                        ds_mapping = ds.copy()
                        properties = ds_mapping['properties']
                        ds_mapping.pop('properties')

                        attrs1 = copy.deepcopy(attrs)
                        attrs1.update({ds_mapping['parameter']: ds_mapping})

                        encoding1 = copy.deepcopy(encoding)
                        encoding1.update({ds_mapping['parameter']: properties['encoding']})

                        base_key_dict = {'dataset_id': ds['dataset_id']}

                        # base_key_dict = {k: v for k, v in ds_mapping.items() if k in base_keys}

                        ## Pre-Process data
                        print('Pre-Process data')
                        qual_col = 'quality_code'
                        freq_code = ds_mapping['frequency_interval']
                        parameter = ds_mapping['parameter']
                        precision = int(np.abs(np.log10(encoding1[parameter]['scale_factor'])))

                        ts_data1 = ts_data.reset_index().drop('Measurement', axis=1).rename(columns={'QualityCode': qual_col, 'Value': parameter, 'Site': 'ref', 'DateTime': 'time'}).copy()
                        # ts_data1.station_id = ts_data1.station_id.str.replace(' ', '_').str.replace('/', '')
                        ts_data1[parameter] = pd.to_numeric(ts_data1[parameter], errors='ignore')

                        ## Aggregate data if necessary
                        print('Aggregate Data')

                        # Parameter
                        if freq_code == 'T':
                            grp1 = ts_data1.groupby(['ref', 'time'])
                            data1 = grp1[parameter].mean()

                        else:
                            agg_fun = agg_stat_mapping[ds_mapping['aggregation_statistic']]

                            if agg_fun == 'sum':
                                data1 = grp_ts_agg(ts_data1[['ref', 'time', parameter]], 'ref', 'time', freq_code, agg_fun)
                            else:
                                data1 = grp_ts_agg(ts_data1[['ref', 'time', parameter]], 'ref', 'time', freq_code, agg_fun, True)

                        # Quality code
                        if qual_col in ts_data1.columns:
                            ts_data1[qual_col] = pd.to_numeric(ts_data1[qual_col], errors='coerce', downcast='integer')
                            if freq_code == 'T':
                                qual1 = grp1[qual_col].min()
                            else:
                                qual1 = grp_ts_agg(ts_data1[['ref', 'time', qual_col]], 'ref', 'time', freq_code, 'min')
                            df3 = pd.concat([data1, qual1], axis=1).reset_index().dropna()
                        else:
                            df3 = data1.reset_index().copy()

                        df3['time'] = df3['time'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc').dt.tz_localize(None)

                        ## Get stn_id
                        stn2 = stns2[stns2.ref == row.Site].copy()
                        stn_id = stn2.station_id.iloc[0]
                        ref = stn2.ref.iloc[0]

                        df3['station_id'] = stn_id
                        df3.drop('ref', axis=1, inplace=True)
                        df3['modified_date'] = run_date.tz_localize(None)

                        ## Create original key name
                        print('Compare to last run')
                        # site_id = row.Site.replace(' ', '_').replace('/', '')
                        key_dict = base_key_dict.copy()
                        key_dict.update({'station_id': stn_id})

                        last_key = last_run_key_pattern.format(**key_dict)

                        try:
                            obj1 = s3.get_object(Bucket=param['remote']['bucket'], Key=last_key)
                            b1 = obj1['Body'].read()
                            p_old_one = read_pkl_zstd(b1, False)
                            xr_old_one = xr.open_dataset(p_old_one)
                            old_one = xr_old_one.to_dataframe().drop(['lat', 'lon', 'ref'], axis=1).reset_index()
                            old_one['time'] = old_one.time.dt.round('s')
                            old_one[parameter] = old_one[parameter].round(precision)

                        except:
                            print('No prior data found. All data will be saved.')
                            old_one = pd.DataFrame(columns=df3.columns)

                        ## Compare to previous run
                        df3[parameter] = df3[parameter].round(precision)
                        res1 = compare_dfs(old_one.drop('modified_date', axis=1), df3.drop('modified_date', axis=1), on=['station_id', 'time'])
                        # res1 = compare_dfs(old_one, df3, on=['station_id', 'time'])

                        up1 = pd.concat([res1['diff'], res1['new']])
                        up1[parameter] = pd.to_numeric(up1[parameter], errors='coerce')
                        up1['modified_date'] = run_date.tz_localize(None)

                        all_ones = pd.concat([old_one, up1]).drop_duplicates(subset='time', keep='last').sort_values('time')
                        all_ones[parameter] = pd.to_numeric(all_ones[parameter], errors='coerce')

                        ## Process data
                        if not up1.empty:

                            df4_up = pd.merge(up1, stn2.drop(['geo'], axis=1), on='station_id')
                            df4_all = pd.merge(all_ones, stn2.drop(['geo'], axis=1), on='station_id')

                            if qual_col in df4_all.columns:
                                df4_up[qual_col] = pd.to_numeric(df4_up[qual_col], errors='coerce', downcast='integer')
                                df4_all[qual_col] = pd.to_numeric(df4_all[qual_col], errors='coerce', downcast='integer')
                                ancillary_variables = [qual_col, 'modified_date']
                            else:
                                ancillary_variables = ['modified_date']

                            p_up1 = df_to_xarray(df4_up, nc_type, parameter, attrs1, encoding1, run_date_key, ancillary_variables, True)
                            p_all1 = df_to_xarray(df4_all, nc_type, parameter, attrs1, encoding1, run_date_key, ancillary_variables, True)

                            ## Save updated data
                            print('Save updated data')
                            key_dict.update({'date': run_date_key})
                            skp4 = base_key_pattern.format(**key_dict)

                            s3.put_object(Body=p_up1, Bucket=param['remote']['bucket'], Key=skp4, ContentType='application/zstd', Metadata={'run_date': run_date_key})

                            ## Save last complete data
                            print('Save last complete data')

                            s3.put_object(Body=p_all1, Bucket=param['remote']['bucket'], Key=last_key, ContentType='application/zstd', Metadata={'run_date': run_date_key})

                            ## Process stn data
                            print('Save station data')
                            stn3 = stn2.iloc[0]
                            stn_m = process_real_station(ds['dataset_id'], [float(stn3.lon), float(stn3.lat)], df3, parameter, precision, s3, param['remote']['bucket'], last_key, ref=stn3['ref'])

                            stn4 = orjson.loads(stn_m.json(exclude_none=True))
                            up_stns = update_remote_stations(s3, param['remote']['bucket'], ds['dataset_id'], [stn4], run_date=run_date)
                            # stns_dict[ds['dataset_id']].append(stn4)

                            # save_folder_flag.update([meas])

                        else:
                            print('No new data to update')

        print('--Success!')

    except Exception as err:
        # print(err)
        print(traceback.format_exc())
        email_msg(param['remote']['email']['sender_address'], param['remote']['email']['sender_password'], param['remote']['email']['receiver_address'], 'Failure on tethys-extraction-es-hilltop', traceback.format_exc())
