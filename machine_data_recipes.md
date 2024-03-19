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


    ---------------------------------------------------------------------------

    CPLE_OpenFailedError                      Traceback (most recent call last)

    File fiona\\ogrext.pyx:136, in fiona.ogrext.gdal_open_vector()
    

    File fiona\\_err.pyx:291, in fiona._err.exc_wrap_pointer()
    

    CPLE_OpenFailedError: filepath.gpkg: No such file or directory

    
    During handling of the above exception, another exception occurred:
    

    DriverError                               Traceback (most recent call last)

    Cell In[11], line 2
          1 gdf = (
    ----> 2     gpd.read_file('filepath.gpkg') # can be any io function grom GeoPandas
          3     .assign(IsoTime=lambda gdf: pd.to_datetime(gdf.IsoTime,format='ISO8601'))
          4 )
          5 gdf.head()
    

    File ~\scoop\apps\anaconda3\2024.02-1\App\Lib\site-packages\geopandas\io\file.py:297, in _read_file(filename, bbox, mask, rows, engine, **kwargs)
        294     else:
        295         path_or_bytes = filename
    --> 297     return _read_file_fiona(
        298         path_or_bytes, from_bytes, bbox=bbox, mask=mask, rows=rows, **kwargs
        299     )
        301 else:
        302     raise ValueError(f"unknown engine '{engine}'")
    

    File ~\scoop\apps\anaconda3\2024.02-1\App\Lib\site-packages\geopandas\io\file.py:338, in _read_file_fiona(path_or_bytes, from_bytes, bbox, mask, rows, where, **kwargs)
        335     reader = fiona.open
        337 with fiona_env():
    --> 338     with reader(path_or_bytes, **kwargs) as features:
        339         crs = features.crs_wkt
        340         # attempt to get EPSG code
    

    File ~\scoop\apps\anaconda3\2024.02-1\App\Lib\site-packages\fiona\env.py:457, in ensure_env_with_credentials.<locals>.wrapper(*args, **kwds)
        454     session = DummySession()
        456 with env_ctor(session=session):
    --> 457     return f(*args, **kwds)
    

    File ~\scoop\apps\anaconda3\2024.02-1\App\Lib\site-packages\fiona\__init__.py:305, in open(fp, mode, driver, schema, crs, encoding, layer, vfs, enabled_drivers, crs_wkt, allow_unsupported_drivers, **kwargs)
        302     path = parse_path(fp)
        304 if mode in ("a", "r"):
    --> 305     colxn = Collection(
        306         path,
        307         mode,
        308         driver=driver,
        309         encoding=encoding,
        310         layer=layer,
        311         enabled_drivers=enabled_drivers,
        312         allow_unsupported_drivers=allow_unsupported_drivers,
        313         **kwargs
        314     )
        315 elif mode == "w":
        316     colxn = Collection(
        317         path,
        318         mode,
       (...)
        327         **kwargs
        328     )
    

    File ~\scoop\apps\anaconda3\2024.02-1\App\Lib\site-packages\fiona\collection.py:243, in Collection.__init__(self, path, mode, driver, schema, crs, encoding, layer, vsi, archive, enabled_drivers, crs_wkt, ignore_fields, ignore_geometry, include_fields, wkt_version, allow_unsupported_drivers, **kwargs)
        241 if self.mode == "r":
        242     self.session = Session()
    --> 243     self.session.start(self, **kwargs)
        244 elif self.mode in ("a", "w"):
        245     self.session = WritingSession()
    

    File fiona\\ogrext.pyx:588, in fiona.ogrext.Session.start()
    

    File fiona\\ogrext.pyx:143, in fiona.ogrext.gdal_open_vector()
    

    DriverError: filepath.gpkg: No such file or directory



```python
crs = f'EPSG:{gdf.crs.to_epsg()}'
crs
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    Cell In[12], line 1
    ----> 1 crs = f'EPSG:{gdf.crs.to_epsg()}'
          2 crs
    

    NameError: name 'gdf' is not defined


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




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>section_track</th>
      <th>SECTIONID</th>
      <th>geometry</th>
      <th>datetime</th>
      <th>FUEL</th>
      <th>VEHICLSPEED</th>
      <th>DISTANCE</th>
      <th>Heading</th>
      <th>SWATHWIDTH</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>41368</td>
      <td>LINESTRING (425996.742 8470930.166, 425996.440...</td>
      <td>2023-10-25 14:01:57.644000+00:00</td>
      <td>0.01222425</td>
      <td>7.48971829</td>
      <td>2.07641</td>
      <td>351.269018</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
      <td>41371</td>
      <td>LINESTRING (425997.237 8470930.237, 425996.935...</td>
      <td>2023-10-25 14:01:57.644000+00:00</td>
      <td>0.00499739</td>
      <td>7.48971829</td>
      <td>2.07641</td>
      <td>351.269018</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0</td>
      <td>41374</td>
      <td>LINESTRING (425997.732 8470930.307, 425997.430...</td>
      <td>2023-10-25 14:01:57.644000+00:00</td>
      <td>0.00122440</td>
      <td>7.48971829</td>
      <td>2.07641</td>
      <td>351.269018</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0</td>
      <td>41377</td>
      <td>LINESTRING (425997.925 8470932.419, 425997.617...</td>
      <td>2023-10-25 14:01:58.640000+00:00</td>
      <td>0.00351287</td>
      <td>7.29891829</td>
      <td>2.07766</td>
      <td>351.185078</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0</td>
      <td>41380</td>
      <td>LINESTRING (425998.420 8470932.489, 425998.112...</td>
      <td>2023-10-25 14:01:58.640000+00:00</td>
      <td>0.00160442</td>
      <td>7.29891829</td>
      <td>2.07766</td>
      <td>351.185078</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3357</th>
      <td>80</td>
      <td>60453</td>
      <td>LINESTRING (424836.588 8471346.984, 424839.910...</td>
      <td>2023-10-26 17:31:00.374000+00:00</td>
      <td>0.00082171</td>
      <td>12.00654560</td>
      <td>3.221268</td>
      <td>68.897539</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>3358</th>
      <td>81</td>
      <td>60453</td>
      <td>LINESTRING (424871.357 8471332.287, 424868.608...</td>
      <td>2023-10-26 17:32:04.363000+00:00</td>
      <td>0.00032425</td>
      <td>8.96876612</td>
      <td>3.638225</td>
      <td>187.094515</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>3359</th>
      <td>82</td>
      <td>60453</td>
      <td>LINESTRING (424726.925 8469915.255, 424726.812...</td>
      <td>2023-10-26 17:39:46.304000+00:00</td>
      <td>0.00064466</td>
      <td>7.48781668</td>
      <td>2.365291</td>
      <td>355.218532</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>3360</th>
      <td>83</td>
      <td>60453</td>
      <td>LINESTRING (424724.991 8469940.891, 424725.576...</td>
      <td>2023-10-26 17:39:55.304000+00:00</td>
      <td>0.00055438</td>
      <td>11.91005668</td>
      <td>3.507319</td>
      <td>53.786042</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>3361</th>
      <td>84</td>
      <td>60453</td>
      <td>LINESTRING (424771.734 8470194.692, 424771.299...</td>
      <td>2023-10-26 17:41:10.299000+00:00</td>
      <td>0.00059681</td>
      <td>13.48410935</td>
      <td>3.626575</td>
      <td>155.173314</td>
      <td>0.5</td>
    </tr>
  </tbody>
</table>
<p>3362 rows × 9 columns</p>
</div>



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


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    Cell In[19], line 2
          1 gdf_planter_tracks = (
    ----> 2     gdf
          3     .dissolve(by='IsoTime',as_index=False)
          4     .assign(
          5         geometry=lambda gdf: gdf.centroid,
          6         timestep=lambda gdf: gdf.IsoTime.pipe(timestep),
          7         track=lambda gdf: gdf.timestep.pipe(track),
          8         # tracktimediff_s=lambda gdf: gdf.groupby('track').IsoTime.transform(np.ptp).dt.total_seconds(),
          9         # trackelevationdiff_m=lambda gdf: gdf.groupby('track').Elevation.transform(np.ptp),
         10         # worked_area_ha=lambda gdf: abs(gdf.DISTANCE*gdf.SWATHWIDTH)/10000,
         11         # FUEL=lambda gdf: gdf.FUEL.mul(27), # number of sections, as FUEL is evenly divided by the number of sections
         12     )
         13     .groupby('track').agg({
         14         'geometry':partial(points2linestring,errors='ignore'),
         15         # include any attribute from file and its aggregation fuction. Below are a few examples from a seeding file, uncomment as needed.
         16         # 'IsoTime':'min',
         17         # 'tracktimediff_s':'first',
         18         # 'trackelevationdiff_m':'first',
         19         # 'worked_area_ha':'sum',
         20         # 'FUEL':'sum',
         21         # 'VEHICLSPEED':'mean',
         22         # 'DISTANCE':'sum',
         23         # 'AppliedRate':'mean',
         24         # 'Elevation':'mean',
         25     })
         26     .dropna(subset='geometry')
         27     .pipe(gpd.GeoDataFrame,geometry='geometry',crs=crs)
         28     .pipe(to_parquet,'file.parquet',compression='zstd') # any GeoPandas io function can be used instead 
         29 )
    

    NameError: name 'gdf' is not defined


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


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    Cell In[20], line 2
          1 (
    ----> 2     gdf_section_tracks
          3     .sort_values('IsoTime')
          4     .merge(gdf_machine_tracks.reset_index().loc[:,['IsoTime','track']],on='IsoTime',how='left')
          5     .sort_values(['datetime','track'])
          6     .assign(track=lambda gdf: gdf.track.ffill())
          7     .astype({'track':'int32'})
          8     .pipe(to_file,'output.gpkg',driver='GPKG')
          9 )
    

    NameError: name 'gdf_section_tracks' is not defined


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


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    Cell In[22], line 2
          1 gdf_2020_tracks = (
    ----> 2     gdf_2020
          3     # .assign(
          4     #     # pass_track=lambda gdf: gdf.groupby(['Date','Field','Product','Pass Num'])['Duration(s)'].transform(track),
          5     #     # worked_area_ha=lambda gdf: (gdf.loc[:,'Distance(m)']*18)/10000,
          6     #     # calc_timestep=lambda gdf: gdf.loc[:,'Distance(m)']/gdf.loc[:,'Speed(km/h)'].div(3.6)
          7     # )
          8     .groupby(['Date','Field','Product','Pass Num'],as_index=False).agg({
          9         'geometry':partial(points2linestring,errors='ignore'),
         10         # 'Duration(s)':'sum',
         11         # 'worked_area_ha':'sum',
         12         # 'calc_timestep':'sum',
         13         # 'Speed(km/h)':'mean',
         14         # 'Distance(m)':'sum',
         15         # 'Prod(ha/h)':'mean',
         16         # 'Elevation(m)':'mean',
         17         # 'Track(deg)':'mean',
         18     })
         19     .dropna(subset='geometry')
         20     .pipe(to_parquet,'output.parquet',compression='zstd')
         21 )
    

    NameError: name 'gdf_2020' is not defined

