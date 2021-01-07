"""
Functions to query APIs for altitude data.

"""
import requests
from tethysts import Tethys

######################################
#### Parameters

koordinates_rater_format = '{base_url}/services/query/v1/raster.json?key={key}&layer={layer_id}&x={lon}&y={lat}'

#####################################
### Functions


def koordinates_raster_query(base_url: str, key: str, layer_id: (int, str), lon: float, lat: float):
    """

    """
    url_request = koordinates_rater_format.format(base_url=base_url, key=key, layer_id=layer_id, lon=lon, lat=lat)
    resp = requests.get(url_request)

    if resp.ok:
        layer1 = resp.json()['rasterQuery']['layers'][str(layer_id)]

        status = layer1['status']

        if status == 'ok':
            bands = layer1['bands']

            return bands
        else:
            print('status is: ' + status)
            return None

    else:
        return resp.content.decode()


def get_altitude(stn_df, dataset_id, koordinates_key, tethys_remote=None, missing_value=-9999):
    """

    """
    stn_df1 = stn_df[['station_id', 'lon', 'lat']].drop_duplicates(subset=['station_id']).copy()

    if isinstance(tethys_remote, dict):
        try:
            t1 = Tethys([tethys_remote])
            old_stns = t1.get_stations(dataset_id)
        except:
            old_stns = []
    else:
        old_stns = []

    alt1 = []
    for i, row in stn_df1.iterrows():
        # print(row['station_id'])
        old_stn_alt = [s['altitude'] for s in old_stns if s['station_id'] == row['station_id']]

        if old_stn_alt:
            alt1.extend(old_stn_alt)
        else:
            try:
                r1 = koordinates_raster_query('https://data.linz.govt.nz', koordinates_key, '51768', row.lon, row.lat)[0]['value']
                alt1.extend([round(r1, 3)])
            except:
                print('No altitude found for ' + row['station_id'] + ', using: ' + str(missing_value))
                alt1.extend([missing_value])

    stn_df1['altitude'] = alt1

    return stn_df1

####################################
### Testing

# base_url = 'https://data.linz.govt.nz'
# key = ''
# layer_id = 51768
# lon = 172.084790
# lat = -43.222752
#
#
#
# bands1 = koordinates_raster_query(base_url, key, layer_id, lon, lat)
