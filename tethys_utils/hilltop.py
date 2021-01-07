"""

"""
import os
import io
import requests
import numpy as np
import zstandard as zstd
import pandas as pd
import copy
import xarray as xr
import orjson
from time import sleep
import traceback
# from tethys_utils.data_models import Geometry, Dataset, DatasetBase, S3ObjectKey, Station, Stats
from tethys_utils.main import nc_ts_key_pattern, assign_ds_ids, put_remote_dataset, create_geometry, assign_station_id, grp_ts_agg, read_pkl_zstd, data_to_xarray, process_real_station, put_remote_station, email_msg, s3_connection, agg_stat_mapping, put_remote_agg_datasets, put_remote_agg_stations, list_parse_s3, get_remote_station, compare_datasets_from_s3
from tethys_utils.altitude_io import koordinates_raster_query, get_altitude
import urllib3
from tethysts.utils import key_patterns


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


def get_hilltop_water_use_data(param, ts_local_tz, station_mtype_corrections=None, measurement='Abstraction Volume', tethys_url=None):
    """

    """
    import requests
    from hilltoppy import web_service as ws
    # from hilltoppy import util

    try:

        ### Read in parameters

        base_url = param['source']['api_endpoint']
        hts = param['source']['hts']

        datasets = param['source']['dataset_mapping']

        encoding_keys = ['scale_factor', 'dtype', '_FillValue']
        base_keys = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

        results_key_pattern = key_patterns['results']

        stn_key_pattern = key_patterns['station']

        attrs = {'quality_code': {'standard_name': 'quality_flag', 'long_name': 'NEMS quality code', 'references': 'https://www.lawa.org.nz/media/16580/nems-quality-code-schema-2013-06-1-.pdf'}}

        encoding = {'quality_code': {'dtype': 'int16', '_FillValue': -9999}}

        gauging_measurements = ['Flow [Gauging Results]', 'Stage', 'Area', 'Velocity [Gauging Results]', 'Max Depth', 'Slope', 'Width', 'Hyd Radius', 'Wet. Perimeter', 'Sed. Conc.', 'Temperature', 'Stage Change [Gauging Results]', 'Method', 'Number Verts.', 'Gauge Num.']

        ### Initalize

        run_date = pd.Timestamp.today(tz='utc').round('s')
        run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')

        s3 = s3_connection(param['remote']['connection_config'])

        ### Create dataset_ids, check if datasets.json exist on remote, and if not add it
        for ht_ds, ds_list in datasets.items():
            ds_list2 = assign_ds_ids([ds_list])
            datasets[ht_ds] = ds_list2[0]

        dataset_list = []
        [dataset_list.extend([ds_list]) for ht_ds, ds_list in datasets.items()]

        ### Pull out stations
        stns1 = ws.site_list(base_url, hts, location='LatLong') # There's a problem with Hilltop that requires running the site list without a measurement first...
        stns1 = ws.site_list(base_url, hts, location='LatLong', measurement=measurement)
        stns2 = stns1[(stns1.lat > -47.5) & (stns1.lat < -34) & (stns1.lon > 166) & (stns1.lon < 179)].dropna().copy()
        stns2.rename(columns={'SiteName': 'ref'}, inplace=True)

        stns2['geo'] = stns2.apply(lambda x: create_geometry([x.lon, x.lat]), axis=1)
        stns2['station_id'] = stns2.apply(lambda x: assign_station_id(x.geo), axis=1)

        stns2 = stns2.drop_duplicates('station_id').copy()

        ### Pull out the mtypes
        print('-Running through station/measurement combos')

        mtypes_list = []
        for s in stns2.ref:
            print(s)
            try:
                meas1 = ws.measurement_list(base_url, hts, s, measurement=measurement)
            except:
                print('** station is bad')
            mtypes_list.append(meas1)
        mtypes_df = pd.concat(mtypes_list).reset_index()
        mtypes_df = mtypes_df[mtypes_df.Measurement == measurement].copy()
        mtypes_df.rename(columns={'Site': 'ref'}, inplace=True)
        mtypes_df2 = pd.merge(mtypes_df, stns2, on='ref')

        mtypes_df2['feature'] = 'gw'
        mtypes_df2.loc[mtypes_df2.ref.str.contains('SW/'), 'feature'] = 'sw'

        ## Make corrections to mtypes
        mtypes_df2['corrections'] = False

        if station_mtype_corrections is not None:
            for i, f in station_mtype_corrections.items():
                mtypes_df.loc[(mtypes_df.ref == i[0]) & (mtypes_df.Measurement == i[1]), 'From'] = f
                mtypes_df.loc[(mtypes_df.ref == i[0]) & (mtypes_df.Measurement == i[1]), 'corrections'] = True

        for feat, ds in datasets.items():

            dataset = put_remote_dataset(s3, param['remote']['bucket'], ds, run_date_key)

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

            ## Update sites with altitude
            print('Update sites with altitude either from tethys or koordinates')

            stn_alt = get_altitude(mtypes_df3, ds['dataset_id'], param['source']['koordinates_key'], param['remote'])

            mtypes_df3 = pd.merge(mtypes_df3, stn_alt[['station_id', 'altitude']], on='station_id')

            ## Iterate through the sites/mtypes
            for i, row in mtypes_df3.iterrows():
                print(row.ref)

                print('- Extracting data...')
                bad_error = False
                timer = 5
                while timer > 0:

                    try:
                        sleep(1)
                        if row['corrections']:
                            ts_data = ws.get_data(base_url, hts, row.Site, row.Measurement, from_date=str(row.From), to_date=str(row.To), agg_method='Total', agg_interval='1 day')[1:].reset_index()
                        else:
                            ts_data = ws.get_data(base_url, hts, row.ref, row.Measurement, agg_method='Total', agg_interval='1 day')[1:].reset_index()
                        break
                    except requests.exceptions.ConnectionError as err:
                        print(row.ref + ' and ' + row.Measurement + ' error: ' + str(err))
                        timer = timer - 1
                        sleep(30)
                    except ValueError as err:
                        print(row.ref + ' and ' + row.Measurement + ' error: ' + str(err))
                        bad_error = True
                        break
                    except Exception as err:
                        print(str(err))
                        timer = timer - 1
                        sleep(30)

                if timer == 0:
                    raise ValueError('The Hilltop request tried too many times...the server is probably down')

                if bad_error:
                    continue

                ## Pre-Process data
                print('Pre-Process data')
                stn = mtypes_df3.loc[mtypes_df3.ref == row.ref, ['ref', 'lat', 'lon', 'altitude', 'station_id']].iloc[0].to_dict()

                stn_id = stn['station_id']
                ref = stn['ref']

                mod_date = pd.Timestamp.today(tz='utc').round('s').tz_localize(None)

                ts_data['DateTime'] = ts_data['DateTime'] - pd.DateOffset(days=1)

                ts_data1 = ts_data.drop(['Site', 'Measurement'], axis=1).rename(columns={ 'Value': parameter, 'DateTime': 'time'}).copy()

                ts_data1[parameter] = pd.to_numeric(ts_data1[parameter], errors='coerce')
                ts_data1['time'] = ts_data1['time'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc').dt.tz_localize(None)
                ts_data1['height'] = 0
                ts_data1['modified_date'] = mod_date

                ## Convert to xarray
                df4 = ts_data1.copy()
                df4.set_index(['time', 'height'], inplace=True)

                ancillary_variables = ['modified_date']

                new1 = data_to_xarray(df4, stn, parameter, attrs1, encoding1, run_date=run_date_key, ancillary_variables=ancillary_variables)

                ## Compare to last run
                print('Compare to last run')

                up1 = compare_datasets_from_s3(s3, param['remote']['bucket'], new1)

                ## Process data
                if isinstance(up1, xr.Dataset):

                    ## Save results
                    print('Save results')

                    key_dict = base_key_dict.copy()
                    key_dict.update({'run_date': run_date_key, 'station_id': stn_id})
                    new_key = results_key_pattern.format(**key_dict)

                    cctx = zstd.ZstdCompressor(level=1)
                    c_obj = cctx.compress(up1.to_netcdf())

                    s3.put_object(Body=c_obj, Bucket=param['remote']['bucket'], Key=new_key, ContentType='application/zstd', Metadata={'run_date': run_date_key})

                    ## Process stn data
                    print('Save station data')

                    stn_m = process_real_station(ds['dataset_id'], [float(stn['lon']), float(stn['lat'])], up1.squeeze('height')[parameter].drop('height').to_dataframe().reset_index(), parameter, precision, s3, param['remote']['bucket'], ref=stn['ref'], altitude=stn['altitude'], mod_date=run_date)

                    stn4 = orjson.loads(stn_m.json(exclude_none=True))
                    up_stns = put_remote_station(s3, param['remote']['bucket'], stn4, run_date=run_date)

                else:
                    print('No new data to update')

    except Exception as err:
        # print(err)
        print(traceback.format_exc())
        email_msg(param['remote']['email']['sender_address'], param['remote']['email']['sender_password'], param['remote']['email']['receiver_address'], 'Failure on tethys-extraction-es-hilltop', traceback.format_exc())

    try:

        ### Aggregate all stations for the dataset
        print('Aggregate all stations for the dataset and all datasets in the bucket')

        for ds in dataset_list:
            ds_stations = put_remote_agg_stations(s3, param['remote']['bucket'], ds['dataset_id'])

        ### Aggregate all datasets for the bucket
        ds_all = put_remote_agg_datasets(s3, param['remote']['bucket'])

        print('--Success!')

    except Exception as err:
        # print(err)
        print(traceback.format_exc())
        email_msg(param['remote']['email']['sender_address'], param['remote']['email']['sender_password'], param['remote']['email']['receiver_address'], 'Failure on tethys-extraction-es-hilltop', traceback.format_exc())



def get_qc_hilltop_data(param, ts_local_tz, station_mtype_corrections=None, tethys_url=None):
    """

    """
    import requests
    from hilltoppy import web_service as ws

    try:

        ### Read in parameters

        base_url = param['source']['api_endpoint']
        hts = param['source']['hts']

        datasets = param['source']['dataset_mapping']

        encoding_keys = ['scale_factor', 'dtype', '_FillValue']
        base_keys = ['feature', 'parameter', 'method', 'product_code', 'owner', 'aggregation_statistic', 'frequency_interval', 'utc_offset']

        results_key_pattern = key_patterns['results']

        stn_key_pattern = key_patterns['station']

        attrs = {'quality_code': {'standard_name': 'quality_flag', 'long_name': 'NEMS quality code', 'references': 'https://www.lawa.org.nz/media/16580/nems-quality-code-schema-2013-06-1-.pdf'}}

        encoding = {'quality_code': {'dtype': 'int16', '_FillValue': -9999}}

        gauging_measurements = ['Flow [Gauging Results]', 'Stage', 'Area', 'Velocity [Gauging Results]', 'Max Depth', 'Slope', 'Width', 'Hyd Radius', 'Wet. Perimeter', 'Sed. Conc.', 'Temperature', 'Stage Change [Gauging Results]', 'Method', 'Number Verts.', 'Gauge Num.']

        ### Initalize

        run_date = pd.Timestamp.today(tz='utc').round('s')
        run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')

        s3 = s3_connection(param['remote']['connection_config'])

        ### Create dataset_ids, check if datasets.json exist on remote, and if not add it
        for ht_ds, ds_list in datasets.items():
            ds_list2 = assign_ds_ids(ds_list)
            datasets[ht_ds] = ds_list2

        dataset_list = []
        [dataset_list.extend(ds_list) for ht_ds, ds_list in datasets.items()]

        for meas in datasets:

            print(meas)

            ### Pull out stations
            stns1 = ws.site_list(base_url, hts, location='LatLong') # There's a problem with Hilltop that requires running the site list without a measurement first...
            stns1 = ws.site_list(base_url, hts, location='LatLong', measurement=meas)
            stns2 = stns1[(stns1.lat > -47.5) & (stns1.lat < -34) & (stns1.lon > 166) & (stns1.lon < 179)].dropna().copy()
            stns2.rename(columns={'SiteName': 'ref'}, inplace=True)

            stns2['geo'] = stns2.apply(lambda x: create_geometry([x.lon, x.lat]), axis=1)
            stns2['station_id'] = stns2.apply(lambda x: assign_station_id(x.geo), axis=1)

            stns2 = stns2.drop_duplicates('station_id').copy()

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
            mtypes_df = mtypes_df[mtypes_df.Measurement == meas].copy()

            ## Make corrections to mtypes
            mtypes_df['corrections'] = False

            if station_mtype_corrections is not None:
                for i, f in station_mtype_corrections.items():
                    mtypes_df.loc[(mtypes_df.Site == i[0]) & (mtypes_df.Measurement == i[1]), 'From'] = f
                    mtypes_df.loc[(mtypes_df.Site == i[0]) & (mtypes_df.Measurement == i[1]), 'corrections'] = True

            # save_folder_flag = set()
            stns_dict = {d['dataset_id']: [] for d in dataset_list}

            ## Update sites with altitude
            print('Get Tethys stations if they exist else koordinates')
            stn_alt = get_altitude(stns2, datasets[meas][0]['dataset_id'], param['source']['koordinates_key'], param['remote'])
            stns2 = pd.merge(stns2, stn_alt[['station_id', 'altitude']], on='station_id')

            if not mtypes_df.empty:

                ##  Iterate through each stn
                print('Iterate through each station')
                for i, row in mtypes_df.iterrows():
                    print(row.Site)

                    ## Get the data out
                    print('- Extracting data...')

                    bad_error = False
                    timer = 5
                    while timer > 0:
                        ts_data_list = []

                        try:
                            sleep(1)
                            if row['corrections']:
                                ts_data = ws.get_data(base_url, hts, row.Site, row.Measurement, from_date=str(row.From), to_date=str(row.To), quality_codes=True)
                            else:
                                ts_data = ws.get_data(base_url, hts, row.Site, row.Measurement, quality_codes=True)

                            break
                        except requests.exceptions.ConnectionError as err:
                            print(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            timer = timer - 1
                            sleep(30)
                        except ValueError as err:
                            print(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            bad_error = True
                            break
                        except Exception as err:
                            print(str(err))
                            timer = timer - 1
                            sleep(30)

                        if timer == 0:
                            raise ValueError('The Hilltop request tried too many times...the server is probably down')

                    if bad_error:
                        continue

                    stn = stns2[stns2.ref == row.Site].iloc[0].to_dict()
                    stn.pop('geo')

                    stn_id = stn['station_id']
                    ref = stn['ref']
                    mod_date = pd.Timestamp.today(tz='utc').round('s').tz_localize(None)

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


                        # df3['station_id'] = stn_id
                        df3.drop('ref', axis=1, inplace=True)
                        df3['modified_date'] = mod_date

                        ## Convert to xarray
                        df4 = df3.copy()
                        df4['height'] = 0
                        df4.set_index(['time', 'height'], inplace=True)

                        if qual_col in df4.columns:
                            # df4_up[qual_col] = pd.to_numeric(df4_up[qual_col], errors='coerce', downcast='integer')
                            df4[qual_col] = pd.to_numeric(df4[qual_col], errors='coerce', downcast='integer')
                            ancillary_variables = [qual_col, 'modified_date']
                        else:
                            ancillary_variables = ['modified_date']

                        # p_up1 = df_to_xarray(df4_up, nc_type, parameter, attrs1, encoding1, run_date_key, ancillary_variables, True)
                        new1 = data_to_xarray(df4, stn, parameter, attrs1, encoding1, run_date=run_date_key, ancillary_variables=ancillary_variables)

                        ## Compare to last run
                        print('Compare to last run')

                        up1 = compare_datasets_from_s3(s3, param['remote']['bucket'], new1)

                        ## Process data
                        if isinstance(up1, xr.Dataset):

                            ## Save results
                            print('Save results')
                            key_dict = base_key_dict.copy()
                            key_dict.update({'station_id': stn_id, 'run_date': run_date_key})

                            new_key = results_key_pattern.format(**key_dict)

                            cctx = zstd.ZstdCompressor(level=1)
                            c_obj = cctx.compress(up1.to_netcdf())

                            s3.put_object(Body=c_obj, Bucket=param['remote']['bucket'], Key=new_key, ContentType='application/zstd', Metadata={'run_date': run_date_key})

                            ## Process stn data
                            print('Save station data')

                            stn_m = process_real_station(ds['dataset_id'], [float(stn['lon']), float(stn['lat'])], up1.squeeze('height')[parameter].drop('height').to_dataframe().reset_index(), parameter, precision, s3, param['remote']['bucket'], ref=stn['ref'], altitude=stn['altitude'], mod_date=run_date)

                            stn4 = orjson.loads(stn_m.json(exclude_none=True))
                            up_stns = put_remote_station(s3, param['remote']['bucket'], stn4, run_date=run_date)
                        else:
                            print('No new data to update')

    except Exception as err:
        # print(err)
        print(traceback.format_exc())
        email_msg(param['remote']['email']['sender_address'], param['remote']['email']['sender_password'], param['remote']['email']['receiver_address'], 'Failure on tethys-extraction-es-hilltop', traceback.format_exc())

    try:

        ### Aggregate all stations for the dataset
        print('Aggregate all stations for the dataset and all datasets in the bucket')

        for ds in dataset_list:
            ds_new = put_remote_dataset(s3, param['remote']['bucket'], ds)
            ds_stations = put_remote_agg_stations(s3, param['remote']['bucket'], ds['dataset_id'])

        ### Aggregate all datasets for the bucket
        ds_all = put_remote_agg_datasets(s3, param['remote']['bucket'])

        print('--Success!')

    except Exception as err:
        # print(err)
        print(traceback.format_exc())
        email_msg(param['remote']['email']['sender_address'], param['remote']['email']['sender_password'], param['remote']['email']['receiver_address'], 'Failure on tethys-extraction-es-hilltop', traceback.format_exc())
