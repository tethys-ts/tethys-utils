"""
Functions to query APIs for altitude data.

"""
import requests


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






















