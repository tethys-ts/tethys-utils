"""
Created by Mike Kittridge on 2020-11-02.

For hashing the geometry of sites, use blake2b with a digest_size of 12.
"""
from datetime import datetime, date
from typing import List, Optional, Dict, Union
from pydantic import BaseModel, Field
from hashlib import blake2b


#########################################
### Models


class Stats(BaseModel):
    """
    Statistics related to the results.
    """
    min: float
    max: float
    count: int
    from_date: datetime
    to_date: datetime


class Geometry(BaseModel):
    """
    Geojson-like geometry model.
    """
    type: str
    coordinates: List[float]


class Site(BaseModel):
    """
    Contains the site data of a dataset.
    """
    site_id: str = Field(..., description='site uuid')
    ref: str = None
    name: str = None
    osm_id: int = None
    virtual_site: bool
    geometry: Geometry
    altitude: float = None
    stats: Stats = None
    time_series_object_list: List = None
    properties: Dict = Field(None, description='Any additional site specific properties.')
    modified_date: datetime = Field(None, description='The modification date of the last edit.')


class Dataset(BaseModel):
    """
    Dataset data.
    """
    dataset_id: str = Field(..., description='The unique dataset uuid.')
    site_object_key: str = Field(..., description='The object key to the sites data.')
    feature: str = Field(..., description='The hydrologic feature associated with the dataset.')
    parameter: str = Field(..., description='The recorded observation parameter.')
    method: str = Field(..., description='The way the recorded observation was obtained.')
    processing_code: str = Field(..., description='The code associated with the processing state of the recorded observation.')
    owner: str = Field(..., description='The operator, owner, and/or producer of the associated data.')
    aggregation_statistic: str = Field(..., description='The statistic that defines how the result was calculated. The aggregation statistic is calculated over the time frequency interval associated with the recorded observation.')
    frequency_interval: str = Field(..., description='The frequency that the observation was recorded at. In the form 1H for hourly.')
    utc_offset: str = Field(..., description='The offset time from UTC associated with the frequency_interval. For example, if data was collected daily at 9:00, then the frequency_interval would be 24H and the utc_offset would be 9H. The offset must be smaller than the frequency_interval.')
    units: str = Field(..., description='The units of the result.')
    license: str = Field(..., description='The legal data license associated with the dataset defined by the owner.')
    attribution: str = Field(..., description='The legally required attribution text to be distributed with the data defined by the owner.')
    result_type: str = Field(..., description='The collection where the results will be saved.')
    cf_standard_name: str = Field(None, description='The CF conventions standard name for the parameter.')
    wrf_standard_name: str = Field(None, description='The WRF standard name for the parameter.')
    precision: float = Field(None, description='The decimal precision of the result values.')
    description: str = Field(None, description='Dataset description.')
    properties: Dict = Field(None, description='Any additional dataset specific properties.')
    modified_date: datetime = Field(None, description='The modification date of the last edit.')


class Result(BaseModel):
    """
    A normal time series result.
    """
    site_id: str = Field(..., description='site uuid')
    dataset_id: str = Field(..., description='The unique dataset uuid.')
    from_date: Union[datetime, date] = Field(..., description='The start datetime of the observation.')
    result: Union[str, int, float] = Field(..., description='The recorded observation parameter.')
    censor_code: str = Field(None, description='The censor_code of the observation. E.g. > or <')
    properties: Dict = Field(None, description='Any additional result specific properties.')
    modified_date: datetime = Field(None, description='The modification date of the last edit.')



























