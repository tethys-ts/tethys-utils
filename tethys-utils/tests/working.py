# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:25:41 2019

@author: michaelek
"""
import os
import pandas as pd
from lsrm import ds

pd.options.display.max_columns = 10

#################################################
### Parameters


dataset_path = r'E:\ecan\git\lsrm\lsrm\datasets'

irr_file = 'AQUALINC_NZTM_IRRIGATED_AREA_20160629.pkl.xz'
paw_file = 'LAND_NZTM_NEWZEALANDFUNDAMENTALSOILS.pkl.xz'


#################################################
### Save datasets


irr1.to_pickle(os.path.join(dataset_path, irr_file))

irr2 = irr1.copy()
irr2['geometry'] = irr2['geometry'].simplify(40)

irr2.to_pickle(os.path.join(dataset_path, irr_file))

irr3 = pd.read_pickle(os.path.join(dataset_path, irr_file))

paw1.to_pickle(os.path.join(dataset_path, paw_file))

paw2 = paw1.copy()
paw2['geometry'] = paw2['geometry'].simplify(40)

paw2.to_pickle(os.path.join(dataset_path, paw_file))

paw3 = pd.read_pickle(os.path.join(dataset_path, paw_file))



##################################################
### Other


bound_shp = r'\\fileservices02\ManagedShares\Data\VirtualClimate\examples\waipara.shp'
time_agg = 'W' # Use 'D' for day, 'W' for week, or 'M' for month
agg_ts_fun = 'sum'
grid_res = 1000
buffer_dis = 10000
interp_fun = 'cubic'
precip_et=r'\\fileservices02\ManagedShares\Data\VirtualClimate\vcsn_precip_et_2016-06-06.nc'
time_name='time'
x_name='longitude'
y_name='latitude'
rain_name='rain'
pet_name='pe'
crs=4326
irr_eff_dict={'Drip/micro': 1, 'Unknown': 0.8, 'Gun': 0.8, 'Pivot': 0.8, 'K-line/Long lateral': 0.8, 'Rotorainer': 0.8, 'Solid-set': 0.8, 'Borderdyke': 0.5, 'Linear boom': 0.8, 'Unknown': 0.8, 'Lateral': 0.8, 'Wild flooding': 0.5, 'Side Roll': 0.8}
irr_trig_dict={'Drip/micro': 0.7, 'Unknown': 0.5, 'Gun': 0.5, 'Pivot': 0.5, 'K-line/Long lateral': 0.5, 'Rotorainer': 0.5, 'Solid-set': 0.5, 'Borderdyke': 0.5, 'Linear boom': 0.5, 'Unknown': 0.5, 'Lateral': 0.5, 'Wild flooding': 0.5, 'Side Roll': 0.5}
min_irr_area_ratio=0.01
irr_mons=[10, 11, 12, 1, 2, 3, 4]
precip_correction=1.1


self = ds.LSRM()

l2 = self.soils_import()

mv, sites = self.input_processing(bound_shp, grid_res, buffer_dis, interp_fun, agg_ts_fun, time_agg)

results1 = self.lsrm()






