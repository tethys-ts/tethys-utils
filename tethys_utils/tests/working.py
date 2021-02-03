# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:25:41 2019

@author: michaelek
"""
import smtplib
import ssl
import socket
import requests
import tethys_utils as tu
import xarray as xr
from time import time

#################################################
### Parameters

url1 = 'https://b2.tethys-ts.xyz/file/ecan-env-monitoring/tethys/v2/027b6fcae096f053c44c4b4e/1500fc71aab1fb7d0d4786a0/20201115T223158Z/results.nc.zst'
url2 = 'https://f002.backblazeb2.com/file/ecan-env-monitoring/tethys/v2/027b6fcae096f053c44c4b4e/272930c33211b276fc28c1e4/20201115T222429Z/results.nc.zst'
# extra_str = 'file/ecan-env-monitoring/tethys/v2/027b6fcae096f053c44c4b4e/1500fc71aab1fb7d0d4786a0/20201115T223158Z/results.nc.zst'

base_url = 'https://b2.tethys-ts.xyz'
connection_config = base_url
bucket = 'ecan-env-monitoring'
obj_key = 'tethys/v2/027b6fcae096f053c44c4b4e/272930c33211b276fc28c1e4/20201115T222429Z/results.nc.zst'
compression='zstd'


start = time()
for i in range(0, 20):
    print(i)

    r = requests.get(url1)

    data1 = xr.open_dataset(tu.read_pkl_zstd(r.content, False))

end = time()

print(end - start)


start = time()
for i in range(0, 20):
    print(i)

    r = requests.get(url2)

    data1 = xr.open_dataset(tu.read_pkl_zstd(r.content, False))

end = time()

print(end - start)

