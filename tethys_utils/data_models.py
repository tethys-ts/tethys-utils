"""
Created by Mike Kittridge on 2020-11-02.

For hashing the geometry of stations, use blake2b with a digest_size of 12.
Similar for the dataset_id, except that the first 8 fields (starting with feature) should be used for the hashing.
"""
from datetime import datetime, date
from typing import List, Optional, Dict, Union
from pydantic import BaseModel, Field
# from hashlib import blake2b


#########################################
### Models


class Stats(BaseModel):
    """
    Statistics related to the results.
    """
    min: float
    max: float
    mean: float
    median: float
    count: int
    from_date: datetime
    to_date: datetime


class Geometry(BaseModel):
    """
    Geojson-like geometry model.
    """
    type: str
    coordinates: Union[List[float], List[list]]


class S3ObjectKey(BaseModel):
    """
    S3 object key and associated metadata.
    """
    key: str
    bucket: str
    content_length: int
    etag: str = None
    run_date: datetime

class StationBase(BaseModel):
    """
    Contains the base station data.
    """
    station_id: str = Field(..., description='station uuid based on the geometry')
    ref: str = None
    name: str = None
    osm_id: int = None
    geometry: Geometry
    altitude: float = None
    properties: Dict = Field(None, description='Any additional station specific properties.')


class Station(BaseModel):
    """
    Contains the station data of a dataset.
    """
    dataset_id: str = Field(..., description='The unique dataset uuid.')
    station_id: str = Field(..., description='station uuid based on the geometry')
    ref: str = None
    name: str = None
    osm_id: int = None
    geometry: Geometry
    altitude: float = None
    stats: Stats
    results_object_key: List[S3ObjectKey]
    properties: Dict = Field(None, description='Any additional station specific properties.')
    modified_date: datetime = Field(..., description='The modification date of the last edit.')


class Dataset(BaseModel):
    """
    Dataset data.
    """
    dataset_id: str = Field(..., description='The unique dataset uuid.')
    feature: str = Field(..., description='The hydrologic feature associated with the dataset.')
    parameter: str = Field(..., description='The recorded observation parameter.')
    method: str = Field(..., description='The way the recorded observation was obtained.')
    product_code: str = Field(..., description='The code associated with kind of product produced. This could be generic codes like raw_data or quality_controlled_data; or it could be more uniquely identifying the simulation product that was produced.')
    owner: str = Field(..., description='The operator, owner, and/or producer of the associated data.')
    aggregation_statistic: str = Field(..., description='The statistic that defines how the result was calculated. The aggregation statistic is calculated over the time frequency interval associated with the recorded observation.')
    frequency_interval: str = Field(..., description='The frequency that the observation was recorded at. In the form 1H for hourly or 24H for daily. A value of T indicates that the data is saved at its instantaneous measured value (usually down to the minute precision).')
    utc_offset: str = Field(..., description='The offset time from UTC associated with the frequency_interval. For example, if data was collected daily at 9:00 in the timezone of UTC+12, then the frequency_interval would be 24H and the utc_offset would be -3H. This parameter is most important for frequency intervals equal to or greater than 24H and must allign the day with UTC. The offset must be smaller than the frequency_interval.')
    units: str = Field(..., description='The units of the result.')
    license: str = Field(..., description='The legal data license associated with the dataset defined by the owner.')
    attribution: str = Field(..., description='The legally required attribution text to be distributed with the data defined by the owner.')
    # result_type: str = Field(None, description='The type of result. This is a category to define how the station results can or cannot be handled on the whole. For example, a time_series_grid would indicate that the stations are alligned in a grid.')
    spatial_distribution: str = Field(..., description='This describes how the spatial data are distributed. Either sparse or grid.')
    geometry_type: str = Field(..., description='This describes how the spatial dimensions are stored. Point, Line, Polygon, or Collection. Follows the OGC spatial data types.')
    grouping: str = Field(..., description='This describes how the staions and the associated data are grouped. Either none or blocks.')
    extent: Geometry = Field(None, description='The geographical extent of the datset as a simple rectangular polygon.')
    cf_standard_name: str = Field(None, description='The CF conventions standard name for the parameter.')
    wrf_standard_name: str = Field(None, description='The WRF standard name for the parameter.')
    precision: float = Field(None, description='The decimal precision of the result values.')
    description: str = Field(None, description='Dataset description.')
    processing_code: int = Field(None, description='The processing code to determine how the input data should be processed.')
    properties: Dict = Field(None, description='Any additional dataset specific properties.')
    modified_date: datetime = Field(None, description='The modification date of the last edit.')


class DatasetBase(BaseModel):
    """
    Core fields in the dataset.
    """
    feature: str = Field(..., description='The hydrologic feature associated with the dataset.')
    parameter: str = Field(..., description='The recorded observation parameter.')
    method: str = Field(..., description='The way the recorded observation was obtained.')
    product_code: str = Field(..., description='The code associated with kind of product produced. This could be generic codes like raw_data or quality_controlled; or it could be more uniquely identifying the simulation product that was produced.')
    owner: str = Field(..., description='The operator, owner, and/or producer of the associated data.')
    aggregation_statistic: str = Field(..., description='The statistic that defines how the result was calculated. The aggregation statistic is calculated over the time frequency interval associated with the recorded observation.')
    frequency_interval: str = Field(..., description='The frequency that the observation was recorded at. In the form 1H for hourly.')
    utc_offset: str = Field(..., description='The offset time from UTC associated with the frequency_interval. For example, if data was collected daily at 9:00, then the frequency_interval would be 24H and the utc_offset would be 9H. The offset must be smaller than the frequency_interval.')


# class Result(BaseModel):
#     """
#     A normal time series result.
#     """
#     dataset_id: str = Field(..., description='The unique dataset uuid.')
#     station_id: str = Field(..., description='station uuid')
#     from_date: Union[datetime, date] = Field(..., description='The start datetime of the observation.')
#     result: Union[str, int, float] = Field(..., description='The recorded observation parameter.')
#     quality_code: str = Field(None, description='The censor_code of the observation. E.g. > or <')
#     censor_code: str = Field(None, description='The censor_code of the observation. E.g. > or <')
#     properties: Dict = Field(None, description='Any additional result specific properties.')
#     modified_date: datetime = Field(None, description='The modification date of the last edit.')


############################################
### Testing

# import fastjsonschema
#
# station_schema = station.schema()
#
# geometry = {'type': 'Point', 'coordinates': [10000.2334, 344532.3451]}
#
# station_dict = dict(station_id='123', virtual_station=False, geometry=geometry)
#
# station1 = station(**station_dict)
#
# station(station_id='123', virtual_station=False, geometry=geometry).dict(exclude_none=True)
#
#
# val1 = fastjsonschema.compile(station_schema)
#
# val1(station_dict)
