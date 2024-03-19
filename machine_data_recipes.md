# John Deere Machine documentation recipes


```python
import geopandas as gpd
import pandas as pd
import numpy as np
from functools import partial
from shapely import LineString, MultiLineString, MultiPoint, Point, GEOSException
```


```python
def to_clipboard(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    '''Wrapper for pd.DataFrame.to_clipboard to be used with pd.DataFrame.pipe.'''
    df.to_clipboard(**kwargs)
    return df
```


```python
def to_file(gdf: gpd.GeoDataFrame, *args, **kwargs) -> gpd.GeoDataFrame:
    '''Wrapper for gpd.GeoDataFrame.to_file to be used with pd.DataFrame.pipe.'''
    gdf.to_file(*args, **kwargs)
    return gdf
```


```python
def to_feather(gdf: gpd.GeoDataFrame, *args, **kwargs) -> gpd.GeoDataFrame:
    '''Wrapper for gpd.GeoDataFrame.to_feather to be used with pd.DataFrame.pipe.'''
    gdf.to_feather(*args, **kwargs)
    return gdf
```


```python
def to_parquet(gdf: gpd.GeoDataFrame, *args, **kwargs) -> gpd.GeoDataFrame:
    '''Wrapper for gpd.GeoDataFrame.to_parquet to be used with pd.DataFrame.pipe.'''
    gdf.to_parquet(*args, **kwargs)
    return gdf
```


```python
def points2linestring(gser: gpd.GeoSeries, errors: str = 'raise') -> gpd.GeoSeries:
    '''Joins a shapely.Point geometry series to a shapely.LineString. 
    Raises exception if point array contains 1 element.'''
    if len(gser)==1 and errors=='raise':
        raise GEOSException('point array must contain 0 or >1 elements')
    elif len(gser)==1 and errors=='ignore':
        return np.nan
    return LineString(gser.to_numpy())
```


```python
def multipoints2multilinestring(gser: gpd.GeoSeries, errors: str = 'raise') -> gpd.GeoSeries:
    '''Joins a shapely.MultiPoint geometry series to a shapely.LineString. 
    Geometries must be homogeneous.'''
    # loop over multipoints to create a list of points
    lines = []
    for mp in gser:
        line = []
        for p in mp.geoms:
            line.append(p)
        lines.append(line)
    # create a pd.DataFrame to make it easy to pass the lines (now corresponding to each column of the df) to shapely.LineString 
    lines = pd.DataFrame(lines)
    # loop over the columns to create the LineStrings, dropping nans
    linestrings=[]
    for _, line in lines.items():
        line = line.dropna()
        # handle cases where there is only one vertex, which we can't make a line with
        if len(line)==1 and errors=='raise':
            raise GEOSException('point array must contain 0 or >1 elements')
        elif len(line)==1 and errors=='ignore':
            continue
        linestrings.append(LineString(line.to_numpy()))
    return MultiLineString(linestrings)
```


```python
def timestep(ser: pd.Series) -> pd.Series:
    '''Create timestep between each feature in file.
    Same as pd.Series.diff(periods=1), but returns total_seconds from a Datatime series.'''
    return (ser-ser.shift()).dt.total_seconds()
```


```python
def track(ser: pd.Series, thresh: int = 2) -> pd.Series:
    '''Create tracks that are contiguosly less than thresh. 
    Tracks are divided when pd.Series is greater than thresh.'''
    return ser.gt(2).cumsum()
```

- Highly recommended to convert shapefile as downloaded from OperationsCenter to GPKG or geoparquet/geoarrow with GDAL's ogr2ogr, as GeoPandas may become stuck when reading very large shapefiles. If using geoparquet/geoarrow (which significantly speeds io), beware that GeoPandas only reads geometries encoded in WKB, so make sure to pass -lco "GEOMETRY_ENCODING=WKB".
  
  `ogr2ogr -t_srs EPSG:32721 -progress -f "Arrow" -lco "GEOMETRY_ENCODING=WKB" -lco "COMPRESSION=ZSTD" "folder/output.arrow" "folder/input.shp"`
  
  `ogr2ogr output.gpkg input.shp`
[More examples](https://gdal.org/programs/ogr2ogr.html#examples)

## Read file as downloaded from OperationsCenter


```python
gdf = (
    gpd.read_file('filepath.gpkg') # can be any io function grom GeoPandas
    .assign(IsoTime=lambda gdf: pd.to_datetime(gdf.IsoTime,format='ISO8601'))
)
gdf.head()
```


```python
crs = f'EPSG:{gdf.crs.to_epsg()}'
crs
```

## Create section tracks based on SECTIONID and IsoTime attributes


```python
gdf_section_tracks = (
    gdf
    .sort_values('IsoTime')
    .assign(
        section_timestep=lambda gdf: gdf.groupby('SECTIONID').IsoTime.transform(timestep),
        section_track=lambda gdf: gdf.groupby('SECTIONID').section_timestep.transform(track),
    )
    .groupby(['section_track','SECTIONID'],as_index=False).agg({
        'geometry':partial(points2linestring,errors='ignore'),
        # include any attribute from file and its aggregation fuction. Below are a few examples from a seeding file, uncomment as needed.
        # 'IsoTime':'min',
        # 'FUEL':'sum',
        # 'VEHICLSPEED':'mean',
        # 'DISTANCE':'mean',
        # 'Heading':'mean',
        # 'SWATHWIDTH':'mean',
    })
    .dropna(subset='geometry') # drop null geometries
    .pipe(lambda df: gpd.GeoDataFrame(df,geometry='geometry',crs=crs))
    .pipe(to_parquet,'file.parquet',compression='zstd') # any GeoPandas io function can be used instead 
)
```

## Create machine tracks based on IsoTime attribute
If documentation file has sections and section control was active, the track will be shifted to the middle of the recorded sections. 
This will cause the track to become distorted when sections are turned off. See next recipe for a solution.


```python
gdf_machine_tracks = (
    gdf
    .dissolve(by='IsoTime',as_index=False)
    .assign(
        geometry=lambda gdf: gdf.centroid,
        timestep=lambda gdf: gdf.IsoTime.pipe(timestep),
        track=lambda gdf: gdf.timestep.pipe(track),
        # tracktimediff_s=lambda gdf: gdf.groupby('track').IsoTime.transform(np.ptp).dt.total_seconds(),
        # trackelevationdiff_m=lambda gdf: gdf.groupby('track').Elevation.transform(np.ptp),
        # worked_area_ha=lambda gdf: abs(gdf.DISTANCE*gdf.SWATHWIDTH)/10000,
        # FUEL=lambda gdf: gdf.FUEL.mul(27), # number of sections, as FUEL is evenly divided by the number of sections
    )
    .groupby('track').agg({
        'geometry':partial(points2linestring,errors='ignore'),
        # include any attribute from file and its aggregation fuction. Below are a few examples from a seeding file, uncomment as needed.
        # 'IsoTime':'min',
        # 'tracktimediff_s':'first',
        # 'trackelevationdiff_m':'first',
        # 'worked_area_ha':'sum',
        # 'FUEL':'sum',
        # 'VEHICLSPEED':'mean',
        # 'DISTANCE':'sum',
        # 'AppliedRate':'mean',
        # 'Elevation':'mean',
    })
    .dropna(subset='geometry')
    .pipe(gpd.GeoDataFrame,geometry='geometry',crs=crs)
    .pipe(to_parquet,'file.parquet',compression='zstd') # any GeoPandas io function can be used instead 
)
```

## Merge machine tracks with section tracks, and make dissolving each machine track independently possible


```python
(
    gdf_section_tracks
    .sort_values('IsoTime')
    .merge(gdf_machine_tracks.reset_index().loc[:,['IsoTime','track']],on='IsoTime',how='left')
    .sort_values(['datetime','track'])
    .assign(track=lambda gdf: gdf.track.ffill())
    .astype({'track':'int32'})
    .pipe(to_file,'output.gpkg',driver='GPKG')
)
```

---

# 2020 as exported by SMS machine documentation recipes

## Read file as exported by SMS 


```python
gdf_2020 = (
    gpd.read_parquet("input.parquet")
    # .loc[:,[
    #     'Field',
    #     'Dataset',
    #     'Product',
    #     'Obj. Id',
    #     'Date',
    #     'Duration(s)',
    #     'Area Count',
    #     'Elevation(m)',
    #     'Swth Wdth(m)',
    #     'Speed(km/h)',
    #     'Distance(m)',
    #     'Track(deg)',
    #     'Pass Num',
    #     'Prod(ha/h)',
    #     'geometry',
    # ]]
    # .sort_values(['Date','Field','Product','Pass Num','Obj. Id'])
)
```

## Machine tracks based on Pass Num attribute


```python
gdf_2020_tracks = (
    gdf_2020
    # .assign(
    #     # pass_track=lambda gdf: gdf.groupby(['Date','Field','Product','Pass Num'])['Duration(s)'].transform(track),
    #     # worked_area_ha=lambda gdf: (gdf.loc[:,'Distance(m)']*18)/10000,
    #     # calc_timestep=lambda gdf: gdf.loc[:,'Distance(m)']/gdf.loc[:,'Speed(km/h)'].div(3.6)
    # )
    .groupby(['Date','Field','Product','Pass Num'],as_index=False).agg({
        'geometry':partial(points2linestring,errors='ignore'),
        # 'Duration(s)':'sum',
        # 'worked_area_ha':'sum',
        # 'calc_timestep':'sum',
        # 'Speed(km/h)':'mean',
        # 'Distance(m)':'sum',
        # 'Prod(ha/h)':'mean',
        # 'Elevation(m)':'mean',
        # 'Track(deg)':'mean',
    })
    .dropna(subset='geometry')
    .pipe(to_parquet,'output.parquet',compression='zstd')
)
```
