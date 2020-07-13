
#!/usr/bin/env python

"""
Copyright Google Inc. 2017
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

######################################################################################
#     Helper Functions                                                               #
######################################################################################

GOES_PUBLIC_BUCKET='gcp-public-data-goes-16'
MESSAGE_FROM_PUBSUB=''

import apache_beam as beam

def copy_fromgcs(bucket, objectId, destdir):
   import os.path
   import logging
   import google.cloud.storage as gcs
   bucket = gcs.Client().get_bucket(bucket)
   blob = bucket.blob(objectId)
   basename = os.path.basename(objectId)
   logging.info('Downloading {}'.format(basename))
   dest = os.path.join(destdir, basename)
   blob.download_to_filename(dest)
   return dest

def copy_togcs(localfile, bucket_name, blob_name):
   import os.path
   import logging
   import google.cloud.storage as gcs
   bucket = gcs.Client().get_bucket(bucket_name)
   blob = bucket.blob(blob_name)
   blob.upload_from_filename(localfile)
   logging.info('{} uploaded to gs://{}/{}'.format(localfile,
        bucket_name, blob_name))
   return blob


def convert_to_csv(ncfilename,objectId):
    import numpy as np
    from netCDF4 import Dataset
    import csv
    import logging
    import tempfile
    import os.path

    tempdir = tempfile.mkdtemp()
    csvfile_path = tempdir+'test.csv'
    logging.info('In convert_to_csv Reading MESSAGE_FROM_PUBSUB {} '.format(MESSAGE_FROM_PUBSUB['name']))

    # Get the keys and values from nc file to be used in csv 
    lstkeys,lstval = get_csv_key_val(ncfilename)   

    with open(csvfile_path, 'w') as fw:
        logging.info('In convert_to_csv - creating csv file')
        writer = csv.writer(fw)
        writer.writerow(lstkeys)
        writer.writerow(lstval)        

    return csvfile_path





   

def if_table_exists(bigquery_client, table_ref):
    from google.cloud.exceptions import NotFound
    try:
        bigquery_client.get_table(table_ref)
        return True
    except NotFound:
        return False


######################################################################################
#     Schema  Functions                                                              #
######################################################################################

def bq_schema() :

  from google.cloud import bigquery

  abi_l1b_radiance = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='nullable'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='nullable'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='nullable'),
        bigquery.SchemaField('timeline_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('scene_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('band_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('geospatial_westbound_longitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_northbound_latitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_eastbound_longitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_southbound_latitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('nominal_satellite_subpoint_lon', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('valid_pixel_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('missing_pixel_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('saturated_pixel_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('undersaturated_pixel_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('min_radiance_value_of_valid_pixels', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_radiance_value_of_valid_pixels', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_radiance_value_of_valid_pixels', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_radiance_value_of_valid_pixels', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_uncorrectable_l0_errors', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('total_size', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('base_url', 'STRING', mode='nullable')]


  abi_l2_cmip = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='nullable'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='nullable'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='nullable'),
        bigquery.SchemaField('timeline_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('scene_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('band_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('geospatial_westbound_longitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_northbound_latitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_eastbound_longitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_southbound_latitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('nominal_satellite_subpoint_lon', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('total_number_of_points', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('valid_pixel_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count', 'INTEGER', mode='nullable'),
        #bigquery.SchemaField('min_reflectance_factor', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('max_reflectance_factor', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('mean_reflectance_factor', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('std_dev_reflectance_factor', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('min_brightness_temperature', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('max_brightness_temperature', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('mean_brightness_temperature', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('std_dev_brightness_temperature', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_uncorrectable_grb_errors', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_uncorrectable_l0_errors', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('total_size', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('base_url', 'STRING', mode='nullable')]

  abi_l2_mcmip = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='nullable'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='nullable'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='nullable'),
        bigquery.SchemaField('timeline_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('scene_id', 'STRING', mode='nullable'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('geospatial_westbound_longitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_northbound_latitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_eastbound_longitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('geospatial_southbound_latitude', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('nominal_satellite_subpoint_lon', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c01', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c02', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c03', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c04', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c05', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c06', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c07', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c08', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c09', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c10', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c11', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c12', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c13', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c14', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c15', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('outlier_pixel_count_c16', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('min_reflectance_factor_c01', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_reflectance_factor_c01', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_reflectance_factor_c01', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_reflectance_factor_c01', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_reflectance_factor_c02', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_reflectance_factor_c02', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_reflectance_factor_c02', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_reflectance_factor_c02', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_reflectance_factor_c03', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_reflectance_factor_c03', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_reflectance_factor_c03', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_reflectance_factor_c03', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_reflectance_factor_c04', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_reflectance_factor_c04', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_reflectance_factor_c04', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_reflectance_factor_c04', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_reflectance_factor_c05', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_reflectance_factor_c05', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_reflectance_factor_c05', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_reflectance_factor_c05', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_reflectance_factor_c06', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_reflectance_factor_c06', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_reflectance_factor_c06', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_reflectance_factor_c06', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c07', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c07', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c07', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c07', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c08', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c08', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c08', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c08', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c09', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c09', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c09', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c09', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c10', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c10', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c10', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c10', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c11', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c11', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c11', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c11', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c12', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c12', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c12', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c12', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c13', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c13', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c13', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c13', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c14', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c14', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c14', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c14', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c15', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c15', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c15', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c15', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('min_brightness_temperature_c16', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('max_brightness_temperature_c16', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('mean_brightness_temperature_c16', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('std_dev_brightness_temperature_c16', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_uncorrectable_grb_errors', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_uncorrectable_l0_errors', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('min_brightness_temperature_c06', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('max_brightness_temperature_c06', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('mean_brightness_temperature_c06', 'FLOAT', mode='nullable'),
        #bigquery.SchemaField('std_dev_brightness_temperature_c06', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('total_size', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('base_url', 'STRING', mode='nullable')]


  glm_l2_lcfa = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='nullable'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='nullable'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='nullable'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='nullable'),
        bigquery.SchemaField('group_time_threshold', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('flash_time_threshold', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('event_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('group_count', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('flash_count', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_navigated_L1b_events', 'FLOAT', mode='nullable'),
        bigquery.SchemaField('percent_uncorrectable_L0_errors', 'FLOAT', mode='nullable'),      
        bigquery.SchemaField('total_size', 'INTEGER', mode='nullable'),
        bigquery.SchemaField('base_url', 'STRING', mode='nullable')]

  noaa_schema = { 
    'abi_l1b_radiance': abi_l1b_radiance,
    'abi_l2_cmip': abi_l2_cmip,
    'abi_l2_mcmip': abi_l2_mcmip,
    'glm_l2_lcfa': glm_l2_lcfa
  }

  return noaa_schema


def get_csv_key_val(ncfilename):
    from netCDF4 import Dataset
    logging.info('In get_csv_key_val - this is the file name {}'.format(MESSAGE_FROM_PUBSUB['name']))
    with Dataset(ncfilename, 'r') as nc:

        if 'ABI-L1b' in MESSAGE_FROM_PUBSUB['name']:
            logging.info('Initializing csv with ABI-L1b')

            abi_l1b_radiance_keys = ["dataset_name",
                    "platform_ID",
                    "orbital_slot",
                    "timeline_id",
                    "scene_id",
                    "band_id",
                    "time_coverage_start",
                    "time_coverage_end",
                    "date_created",
                    "geospatial_westbound_longitude",
                    "geospatial_northbound_latitude",
                    "geospatial_eastbound_longitude",
                    "geospatial_southbound_latitude",
                    "nominal_satellite_subpoint_lon",
                    "valid_pixel_count",
                    "missing_pixel_count",
                    "saturated_pixel_count",
                    "undersaturated_pixel_count",
                    "min_radiance_value_of_valid_pixels",
                    "max_radiance_value_of_valid_pixels",
                    "mean_radiance_value_of_valid_pixels",
                    "std_dev_radiance_value_of_valid_pixels",
                    "percent_uncorrectable_l0_errors",
                    "total_size",
                    "base_url"]

            abi_l1b_radiance_val = [nc.dataset_name,
                  nc.platform_ID,
                  nc.orbital_slot,
                  nc.timeline_id,
                  nc.scene_id,
                  nc.variables['band_id'].units,
                  nc.time_coverage_start,
                  nc.time_coverage_end,
                  nc.date_created,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_westbound_longitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_northbound_latitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_eastbound_longitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_southbound_latitude,
                  nc.variables['nominal_satellite_subpoint_lon'].getValue().tolist(),
                  nc.variables['valid_pixel_count'].getValue().tolist(),
                  nc.variables['missing_pixel_count'].getValue().tolist(),
                  nc.variables['saturated_pixel_count'].getValue().tolist(),
                  nc.variables['undersaturated_pixel_count'].getValue().tolist(),
                  nc.variables['min_radiance_value_of_valid_pixels'].getValue().tolist(),
                  nc.variables['max_radiance_value_of_valid_pixels'].getValue().tolist(),
                  nc.variables['mean_radiance_value_of_valid_pixels'].getValue().tolist(),
                  nc.variables['std_dev_radiance_value_of_valid_pixels'].getValue().tolist(),
                  nc.variables['percent_uncorrectable_L0_errors'].getValue().tolist(),
                  MESSAGE_FROM_PUBSUB['size'],
                  'gs://gcp-public-data-goes-16/{}'.format(MESSAGE_FROM_PUBSUB['name'])]  

            return (abi_l1b_radiance_keys,abi_l1b_radiance_val)

    # TODO : Update with correct schema 

        elif 'ABI-L2-CMIP' in MESSAGE_FROM_PUBSUB['name']:
            logging.info('Initializing csv with ABI-L2-CMIP')

            abi_l2_cmip_keys = ["dataset_name",
                    "platform_ID",
                    "orbital_slot",
                    "timeline_id",
                    "scene_id",
                    "band_id",
                    "time_coverage_start",
                    "time_coverage_end",
                    "date_created",
                    "geospatial_westbound_longitude",
                    "geospatial_northbound_latitude",
                    "geospatial_eastbound_longitude",
                    "geospatial_southbound_latitude",
                    "nominal_satellite_subpoint_lon",
                    "total_number_of_points",
                    "valid_pixel_count",
                    "outlier_pixel_count",
                    #"min_reflectance_factor",
                    #"max_reflectance_factor",
                    #"mean_reflectance_factor",
                    #"std_dev_reflectance_factor",
                    #"min_brightness_temperature",
                    #"max_brightness_temperature",
                    #"mean_brightness_temperature",   
                    #"std_dev_brightness_temperature",
                    "percent_uncorrectable_grb_errors",
                    "percent_uncorrectable_l0_errors",
                    "total_size",
                    "base_url"]
                  

            abi_l2_cmip_val = [nc.dataset_name,
                  nc.platform_ID,
                  nc.orbital_slot,
                  nc.timeline_id,
                  nc.scene_id,
                  nc.variables['band_id'].units,
                  nc.time_coverage_start,
                  nc.time_coverage_end,
                  nc.date_created,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_westbound_longitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_northbound_latitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_eastbound_longitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_southbound_latitude,
                  nc.variables['nominal_satellite_subpoint_lon'].getValue().tolist(),
                  nc.variables['total_number_of_points'].getValue().tolist(),
                  nc.variables['valid_pixel_count'].getValue().tolist(),
                  nc.variables['outlier_pixel_count'].getValue().tolist(),
                  #nc.variables['min_reflectance_factor'].getValue().tolist(),
                  #nc.variables['max_reflectance_factor'].getValue().tolist(),
                  #nc.variables['mean_reflectance_factor'].getValue().tolist(),
                  #nc.variables['std_dev_reflectance_factor'].getValue().tolist(),
                  #nc.variables['min_brightness_temperature'].getValue().tolist(),
                  #nc.variables['max_brightness_temperature'].getValue().tolist(),
                  #nc.variables['mean_brightness_temperature'].getValue().tolist(),
                  #nc.variables['std_dev_brightness_temperature'].getValue().tolist(),
                  nc.variables['percent_uncorrectable_GRB_errors'].getValue().tolist(),
                  nc.variables['percent_uncorrectable_L0_errors'].getValue().tolist(),
                  MESSAGE_FROM_PUBSUB['size'],
                  'gs://gcp-public-data-goes-16/{}'.format(MESSAGE_FROM_PUBSUB['name'])] 

            return (abi_l2_cmip_keys,abi_l2_cmip_val)

    # TODO : Update with correct schema 

        elif 'ABI-L2-MCMIP' in MESSAGE_FROM_PUBSUB['name']:
            logging.info('Initializing csv with ABI-L2-MCMIP')

            abi_l2_mcmip_keys = [ "dataset_name",       
                "platform_ID",        
                "orbital_slot", 
                "timeline_id",
                "scene_id",
                "time_coverage_start",
                "time_coverage_end",
                "date_created",
                "geospatial_westbound_longitude",
                "geospatial_northbound_latitude",
                "geospatial_eastbound_longitude",
                "geospatial_southbound_latitude",
                "nominal_satellite_subpoint_lon",
                "outlier_pixel_count_c01",
                "outlier_pixel_count_c02",
                "outlier_pixel_count_c03",
                "outlier_pixel_count_c04",
                "outlier_pixel_count_c05",
                "outlier_pixel_count_c06",
                "outlier_pixel_count_c07",
                "outlier_pixel_count_c08",
                "outlier_pixel_count_c09",
                "outlier_pixel_count_c10",
                "outlier_pixel_count_c11",
                "outlier_pixel_count_c12",
                "outlier_pixel_count_c13",
                "outlier_pixel_count_c14",
                "outlier_pixel_count_c15",
                "outlier_pixel_count_c16",
                "min_reflectance_factor_c01",
                "max_reflectance_factor_c01",
                "mean_reflectance_factor_c01",
                "std_dev_reflectance_factor_c01",
                "min_reflectance_factor_c02"
                "max_reflectance_factor_c02"
                "mean_reflectance_factor_c02"
                "std_dev_reflectance_factor_c02"
                "min_reflectance_factor_c03"
                "max_reflectance_factor_c03"
                "mean_reflectance_factor_c03",
                "std_dev_reflectance_factor_c03",
                "min_reflectance_factor_c04",
                "max_reflectance_factor_c04",
                "mean_reflectance_factor_c04",
                "std_dev_reflectance_factor_c04",
                "min_reflectance_factor_c05",
                "max_reflectance_factor_c05",
                "mean_reflectance_factor_c05",
                "std_dev_reflectance_factor_c05",
                "min_reflectance_factor_c06",
                "max_reflectance_factor_c06",
                "mean_reflectance_factor_c06",
                "std_dev_reflectance_factor_c06",
                "min_brightness_temperature_c07",
                "max_brightness_temperature_c07",
                "mean_brightness_temperature_c07",
                "std_dev_brightness_temperature_c07",
                "min_brightness_temperature_c08",
                "max_brightness_temperature_c08",
                "mean_brightness_temperature_c08",
                "std_dev_brightness_temperature_c08",
                "min_brightness_temperature_c09",
                "max_brightness_temperature_c09",
                "mean_brightness_temperature_c09",
                "std_dev_brightness_temperature_c09",
                "min_brightness_temperature_c10",
                "max_brightness_temperature_c10",
                "mean_brightness_temperature_c10",
                "std_dev_brightness_temperature_c10",
                "min_brightness_temperature_c07",
                "max_brightness_temperature_c07",
                "mean_brightness_temperature_c07",
                "std_dev_brightness_temperature_c07",
                "min_brightness_temperature_c11",
                "max_brightness_temperature_c11",
                "mean_brightness_temperature_c11",
                "std_dev_brightness_temperature_c11",
                "min_brightness_temperature_c12",
                "max_brightness_temperature_c12",
                "mean_brightness_temperature_c12",
                "std_dev_brightness_temperature_c12",
                "min_brightness_temperature_c13",
                "max_brightness_temperature_c13",
                "mean_brightness_temperature_c13",
                "std_dev_brightness_temperature_c13",
                "min_brightness_temperature_c14",
                "max_brightness_temperature_c14",
                "mean_brightness_temperature_c14",
                "std_dev_brightness_temperature_c14",
                "min_brightness_temperature_c15",
                "max_brightness_temperature_c15",
                "mean_brightness_temperature_c15",
                "std_dev_brightness_temperature_c15",
                "min_brightness_temperature_c16",
                "max_brightness_temperature_c16",
                "mean_brightness_temperature_c16",
                "std_dev_brightness_temperature_c16",
                "percent_uncorrectable_grb_errors",
                "percent_uncorrectable_l0_errors",
                #min_brightness_temperature_C06,
                #max_brightness_temperature_C06,
                #mean_brightness_temperature_C06,
                #std_dev_brightness_temperature_C06,
                "total_size",
                "base_url"]


            abi_l2_mcmip_val = [nc.dataset_name,
                  nc.platform_ID,
                  nc.orbital_slot,
                  nc.timeline_id,
                  nc.scene_id,
                  nc.time_coverage_start,
                  nc.time_coverage_end,
                  nc.date_created,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_westbound_longitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_northbound_latitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_eastbound_longitude,
                  nc.variables['geospatial_lat_lon_extent'].geospatial_southbound_latitude,
                  nc.variables['nominal_satellite_subpoint_lon'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C01'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C02'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C03'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C04'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C05'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C06'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C07'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C08'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C09'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C10'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C11'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C12'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C13'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C14'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C15'].getValue().tolist(),
                  nc.variables['outlier_pixel_count_C16'].getValue().tolist(),
                  nc.variables['min_reflectance_factor_C01'].getValue().tolist(),
                  nc.variables['max_reflectance_factor_C01'].getValue().tolist(),
                  nc.variables['mean_reflectance_factor_C01'].getValue().tolist(),
                  nc.variables['std_dev_reflectance_factor_C01'].getValue().tolist(),
                  nc.variables['min_reflectance_factor_C02'].getValue().tolist(),
                  nc.variables['max_reflectance_factor_C02'].getValue().tolist(),
                  nc.variables['mean_reflectance_factor_C02'].getValue().tolist(),
                  nc.variables['std_dev_reflectance_factor_C02'].getValue().tolist(),
                  nc.variables['min_reflectance_factor_C03'].getValue().tolist(),
                  nc.variables['max_reflectance_factor_C03'].getValue().tolist(),
                  nc.variables['mean_reflectance_factor_C03'].getValue().tolist(),
                  nc.variables['std_dev_reflectance_factor_C03'].getValue().tolist(),
                  nc.variables['min_reflectance_factor_C04'].getValue().tolist(),
                  nc.variables['max_reflectance_factor_C04'].getValue().tolist(),
                  nc.variables['mean_reflectance_factor_C04'].getValue().tolist(),
                  nc.variables['std_dev_reflectance_factor_C04'].getValue().tolist(),
                  nc.variables['min_reflectance_factor_C05'].getValue().tolist(),
                  nc.variables['max_reflectance_factor_C05'].getValue().tolist(),
                  nc.variables['mean_reflectance_factor_C05'].getValue().tolist(),
                  nc.variables['std_dev_reflectance_factor_C05'].getValue().tolist(),
                  nc.variables['min_reflectance_factor_C06'].getValue().tolist(),
                  nc.variables['max_reflectance_factor_C06'].getValue().tolist(),
                  nc.variables['mean_reflectance_factor_C06'].getValue().tolist(),
                  nc.variables['std_dev_reflectance_factor_C06'].getValue().tolist(),                
                  nc.variables['min_brightness_temperature_C07'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C07'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C07'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C07'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C08'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C08'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C08'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C08'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C09'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C09'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C09'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C09'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C10'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C10'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C10'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C10'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C11'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C11'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C11'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C11'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C12'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C12'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C12'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C12'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C13'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C13'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C13'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C13'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C14'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C14'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C14'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C14'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C15'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C15'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C15'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C15'].getValue().tolist(),               
                  nc.variables['min_brightness_temperature_C16'].getValue().tolist(),
                  nc.variables['max_brightness_temperature_C16'].getValue().tolist(),
                  nc.variables['mean_brightness_temperature_C16'].getValue().tolist(),
                  nc.variables['std_dev_brightness_temperature_C16'].getValue().tolist(),               
                  nc.variables['percent_uncorrectable_GRB_errors'].getValue().tolist(),
                  nc.variables['percent_uncorrectable_L0_errors'].getValue().tolist(),  
                  #nc.variables['min_brightness_temperature_C06'].getValue().tolist(),
                  #nc.variables['max_brightness_temperature_C06'].getValue().tolist(),
                  #nc.variables['mean_brightness_temperature_C06'].getValue().tolist(),
                  #nc.variables['std_dev_brightness_temperature_C06'].getValue().tolist()            
                  MESSAGE_FROM_PUBSUB['size'],
                  'gs://gcp-public-data-goes-16/{}'.format(MESSAGE_FROM_PUBSUB['name'])] 

            return (abi_l2_mcmip_keys,abi_l2_mcmip_val)

    # TODO : Update with correct schema 

        elif 'GLM-L2-LCFA' in MESSAGE_FROM_PUBSUB['name']:
            logging.info('Initializing csv with GLM-L2-LCFA')

            glm_l2_lcfa_keys = [ "dataset_name",
                "platform_ID",
                "orbital_slot",
                "time_coverage_start"
                "time_coverage_end",
                "date_created",
                "group_time_threshold",
                "flash_time_threshold",
                "event_count",
                "group_count",
                "flash_count",
                "percent_navigated_L1b_events",
                "percent_uncorrectable_L0_errors",     
                "total_size",
                "base_url" ]

            glm_l2_lcfa_val = [nc.dataset_name,
                  nc.platform_ID,
                  nc.orbital_slot,
                  nc.time_coverage_start,
                  nc.time_coverage_end,
                  nc.date_created,
                  nc.variables["group_time_threshold"].getValue().tolist(),
                  nc.variables["flash_time_threshold"].getValue().tolist(),
                  nc.variables["event_count"].getValue().tolist(),
                  nc.variables["group_count"].getValue().tolist(),
                  nc.variables["flash_count"].getValue().tolist(),
                  nc.variables["percent_navigated_L1b_events"].getValue().tolist(),
                  nc.variables["percent_uncorrectable_L0_errors"].getValue().tolist(),
                  MESSAGE_FROM_PUBSUB['size'],
                  'gs://gcp-public-data-goes-16/{}'.format(MESSAGE_FROM_PUBSUB['name'])] 

            return (glm_l2_lcfa_keys,glm_l2_lcfa_val)

#######################################################
#                  Pipeline Steps                     #      
#######################################################
def bq_load_csv(url):
    import logging
    from google.cloud import bigquery

    logging.info('***In bq_load_csv *** {}'.format(MESSAGE_FROM_PUBSUB))

    ## schema code here 
    schema_input = bq_schema()

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('staging')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.skip_leading_rows = 1

    if 'ABI-L1b' in MESSAGE_FROM_PUBSUB['name']: 
      table_ref = dataset_ref.table('abi_l1b_radiance')
      table = bigquery.Table(table_ref,schema=schema_input['abi_l1b_radiance'])

    elif 'ABI-L2-CMIP' in MESSAGE_FROM_PUBSUB['name']:
      table_ref = dataset_ref.table('abi_l2_cmip')
      table = bigquery.Table(table_ref,schema=schema_input['abi_l2_cmip'])

    elif 'ABI-L2-MCMIP' in MESSAGE_FROM_PUBSUB['name']:
      table_ref = dataset_ref.table('abi_l2_mcmip')
      table = bigquery.Table(table_ref,schema=schema_input['abi_l2_mcmip'])

    elif 'GLM-L2-LCFA' in MESSAGE_FROM_PUBSUB['name']:
      table_ref = dataset_ref.table('glm_l2_lcfa')
      table = bigquery.Table(table_ref,schema=schema_input['glm_l2_lcfa'])

    if not if_table_exists(bigquery_client, table_ref): 
        table =  bigquery_client.create_table(table)

    load_job = bigquery_client.load_table_from_uri(
        url,
        table_ref,
        job_config=job_config)

    assert load_job.job_type == 'load'
    load_job.result()  # Waits for table load to complete.
    assert load_job.state == 'DONE'

    logging.info('**** Bigquery job completed ****')

def goes_to_csv(objectId, outbucket, outfilename):
    import os, shutil, tempfile, subprocess, logging
    import os.path

    if objectId == None:
        logging.error('Skipping GOES object creation since no GCS file specified')
        return

    tmpdir = tempfile.mkdtemp()
    local_file = copy_fromgcs('gcp-public-data-goes-16', objectId, tmpdir)
    logging.info('Local file copied {}'.format(os.path.basename(local_file)))

    # create csv file in temporary dir, then move over to GCS 
    csvfile = convert_to_csv(local_file, objectId)

    # move over
    if outbucket != None:
        copy_togcs(csvfile, outbucket, outfilename)
        outfilename = 'gs://{}/{}'.format(outbucket, outfilename)
    else:
        subprocess.check_call(['mv', csvfile, outfilename])

    # cleanup
    shutil.rmtree(tmpdir)
    logging.info('Created {} from {}'.format(outfilename, os.path.basename(local_file)))

    return([outfilename])


def extract_objectid(message):
  import json,logging
  global MESSAGE_FROM_PUBSUB
  file_found = 0
  try:
    # message is a string in json format, so we need to parse it as json
    #logging.debug(message)
    logging.info('*** In extract_objectid - the message is, {}'.format(message))
    result = json.loads(message)
    logging.info('*** In extract_objectid - file name is,{}'.format(result['name']))
    # filter for specific filenames
    file_names = ['ABI-L1b', 'ABI-L2-CMIP', 'ABI-L2-MCMIP', 'GLM-L2-LCFA']
    for name in file_names:
        if name in result['name']:
            logging.info('**** Found the relevant file for {} ****'.format(name))
            logging.info('**** Actual file name from the PUB SUB Message {} ****'.format(result['name']))
            MESSAGE_FROM_PUBSUB = result
            file_found = 1
            logging.info('**** MESSAGE_FROM_PUBSUB {}'.format(result['id']))
            yield result['name']
            break
    if not file_found:
        logging.warn('Could not find relevant file in the PUB SUB Message - Skipping pipeline steps')
        pass
  except:
    import sys
    logging.warn(sys.exc_info()[0])
    pass

######################################################################################
#     Pipeline Runner                                                                #
######################################################################################


def noaa_pipeline_goes16(bucket, project, runner):

   import datetime, os
   import apache_beam as beam


   OUTPUT_DIR = 'gs://{}/realtime/'.format(bucket)
   options = {
        'staging_location': os.path.join(OUTPUT_DIR, 'tmp', 'staging'),
        'temp_location': os.path.join(OUTPUT_DIR, 'tmp'),
        'job_name': 'goes16-staging' + datetime.datetime.now().strftime('%y%m%d-%H%M%S'),
        'project': project,
        'max_num_workers': 5,
        'autoscaling_algorithm':"THROUGHPUT_BASED",
        'setup_file': './setup.py',
        'teardown_policy': 'TEARDOWN_ALWAYS',
        'save_main_session': True,
        'streaming': True
   }
   opts = beam.pipeline.PipelineOptions(flags=[], **options)
   p = beam.Pipeline(runner, options=opts)
   (p   | 'events' >> beam.io.ReadFromPubSub(subscription='projects/noaa-goes16/subscriptions/testsubscription')
        | 'filter' >> beam.FlatMap(lambda message: extract_objectid(message))
        | 'nc_to_csv' >> beam.Map(lambda objectid: 
             goes_to_csv(
                objectid,bucket,
               'goes/{}'.format(os.path.basename(objectid).replace('.nc','.csv') ) 
                ))
        | 'load_to_bq' >> beam.Map(lambda url: bq_load_csv(url))
   )
   job = p.run()
   if runner == 'DirectRunner':
      job.wait_until_finish()


if __name__ == '__main__':
   import argparse, logging
   parser = argparse.ArgumentParser(description='Plot images at a specific location in near-real-time')
   parser.add_argument('--bucket', required=True, help='Specify GCS bucket in which to save images')
   parser.add_argument('--project',required=True, help='Specify GCP project to bill')
  
   
   opts = parser.parse_args()
   runner = 'DataflowRunner' # run on Cloud
   #runner = 'DirectRunner' # run Beam on local machine, but write outputs to cloud
   logging.basicConfig(level=getattr(logging, 'INFO', None))

   noaa_pipeline_goes16(opts.bucket, opts.project, runner)



