
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
    logging.info('MESSAGE_FROM_PUBSUB {} '.format(MESSAGE_FROM_PUBSUB['name']))
    logging.info('MESSAGE_FROM_PUBSUB {} '.format(MESSAGE_FROM_PUBSUB['size']))

    # Get the keys and values from nc file to be used in csv 
    lstkeys,lstval = get_csv_key_val(ncfilename)   

    with open(csvfile_path, 'w') as fw:
        writer = csv.writer(fw)
        writer.writerow(lstkeys)
        writer.writerow(lstval)        

    return csvfile_path

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

  try:
    # message is a string in json format, so we need to parse it as json
    #logging.debug(message)
    logging.info('*** In extract_objectid - the message is, {}'.format(message))
    result = json.loads(message)
    logging.info('*** In extract_objectid - file name is,{}'.format(result['name']))
    MESSAGE_FROM_PUBSUB = result
    logging.info('**** MESSAGE_FROM_PUBSUB {}'.format(result['id']))
    yield result['name'] 
  except:
    import sys
    logging.warn(sys.exc_info()[0])
    pass

def bq_load_csv(url):
    import logging
    from google.cloud import bigquery

    logging.info('***In bq_load_csv *** {}'.format(MESSAGE_FROM_PUBSUB))

    ## schema code here 
    schema_input = bq_schema()

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('test')
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
        bigquery.SchemaField('dataset_name', 'STRING', mode='required'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='required'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='required'),
        bigquery.SchemaField('timeline_id', 'STRING', mode='required'),
        bigquery.SchemaField('scene_id', 'STRING', mode='required'),
        bigquery.SchemaField('band_id', 'STRING', mode='required'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('geospatial_westbound_longitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_northbound_latitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_eastbound_longitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_southbound_latitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('nominal_satellite_subpoint_lon', 'FLOAT', mode='required'),
        bigquery.SchemaField('valid_pixel_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('missing_pixel_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('saturated_pixel_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('undersaturated_pixel_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('min_radiance_value_of_valid_pixels', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_radiance_value_of_valid_pixels', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_radiance_value_of_valid_pixels', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_radiance_value_of_valid_pixels', 'FLOAT', mode='required'),
        bigquery.SchemaField('percent_uncorrectable_l0_errors', 'FLOAT', mode='required'),
        bigquery.SchemaField('total_size', 'INTEGER', mode='required'),
        bigquery.SchemaField('base_url', 'STRING', mode='required')]


  abi_l2_cmip = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='required'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='required'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='required'),
        bigquery.SchemaField('timeline_id', 'STRING', mode='required'),
        bigquery.SchemaField('scene_id', 'STRING', mode='required'),
        bigquery.SchemaField('band_id', 'STRING', mode='required'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('geospatial_westbound_longitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_northbound_latitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_eastbound_longitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_southbound_latitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('nominal_satellite_subpoint_lon', 'FLOAT', mode='required'),
        bigquery.SchemaField('total_number_of_points', 'INTEGER', mode='required'),
        bigquery.SchemaField('valid_pixel_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('min_reflectance_factor', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature', 'FLOAT', mode='required'),
        bigquery.SchemaField('percent_uncorrectable_grb_errors', 'FLOAT', mode='required'),
        bigquery.SchemaField('percent_uncorrectable_l0_errors', 'FLOAT', mode='required'),
        bigquery.SchemaField('total_size', 'INTEGER', mode='required'),
        bigquery.SchemaField('base_url', 'STRING', mode='required')]

  abi_l2_mcmip = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='required'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='required'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='required'),
        bigquery.SchemaField('timeline_id', 'STRING', mode='required'),
        bigquery.SchemaField('scene_id', 'STRING', mode='required'),
        bigquery.SchemaField('band_id', 'STRING', mode='required'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('geospatial_westbound_longitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_northbound_latitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_eastbound_longitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('geospatial_southbound_latitude', 'FLOAT', mode='required'),
        bigquery.SchemaField('nominal_satellite_subpoint_lon', 'FLOAT', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c01', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c02', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c03', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c04', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c05', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c06', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c07', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c08', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c09', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c10', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c11', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c12', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c13', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c14', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c15', 'INTEGER', mode='required'),
        bigquery.SchemaField('outlier_pixel_count_c16', 'INTEGER', mode='required'),
        bigquery.SchemaField('min_reflectance_factor_c01', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor_c01', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor_c01', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor_c01', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_reflectance_factor_c02', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor_c02', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor_c02', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor_c02', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_reflectance_factor_c03', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor_c03', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor_c03', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor_c03', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_reflectance_factor_c04', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor_c04', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor_c04', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor_c04', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_reflectance_factor_c05', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor_c05', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor_c05', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor_c05', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_reflectance_factor_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_reflectance_factor_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_reflectance_factor_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_reflectance_factor_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c08', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c08', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c08', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c08', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c09', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c09', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c09', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c09', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c10', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c10', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c10', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c10', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c07', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c11', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c11', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c11', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c11', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c12', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c12', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c12', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c12', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c13', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c13', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c13', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c13', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c14', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c14', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c14', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c14', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c15', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c15', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c15', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c15', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c16', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c16', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c16', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c16', 'FLOAT', mode='required'),
        bigquery.SchemaField('percent_uncorrectable_l0_errors', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c16', 'FLOAT', mode='required'),
        bigquery.SchemaField('min_brightness_temperature_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('max_brightness_temperature_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('mean_brightness_temperature_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('std_dev_brightness_temperature_c06', 'FLOAT', mode='required'),
        bigquery.SchemaField('total_size', 'INTEGER', mode='required'),
        bigquery.SchemaField('base_url', 'STRING', mode='required')]


  glm_l2_lcfa = [
        bigquery.SchemaField('dataset_name', 'STRING', mode='required'),
        bigquery.SchemaField('platform_ID', 'STRING', mode='required'),
        bigquery.SchemaField('orbital_slot', 'STRING', mode='required'),
        bigquery.SchemaField('time_coverage_start', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('time_coverage_end', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('date_created', 'TIMESTAMP', mode='required'),
        bigquery.SchemaField('group_time_threshold', 'FLOAT', mode='required'),
        bigquery.SchemaField('flash_time_threshold', 'FLOAT', mode='required'),
        bigquery.SchemaField('event_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('group_count', 'INTEGER', mode='required'),
        bigquery.SchemaField('flash_count', 'FLOAT', mode='required'),
        bigquery.SchemaField('percent_navigated_L1b_events', 'FLOAT', mode='required'),
        bigquery.SchemaField('percent_uncorrectable_L0_errors', 'FLOAT', mode='required'),      
        bigquery.SchemaField('total_size', 'INTEGER', mode='required'),
        bigquery.SchemaField('base_url', 'STRING', mode='required')]

  noaa_schema = { 
    'abi_l1b_radiance': abi_l1b_radiance,
    'abi_l2_cmip': abi_l2_cmip,
    'abi_l2_mcmip': abi_l2_mcmip,
    'glm_l2_lcfa': glm_l2_lcfa
  }

  return noaa_schema


def get_csv_key_val(ncfilename):
  from netCDF4 import Dataset

  with Dataset(ncfilename, 'r') as nc:

    if 'ABI-L1b' in MESSAGE_FROM_PUBSUB['name']:

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

        return (abi_l2_cmip_keys,abi_l2_cmip_val)

    # TODO : Update with correct schema 

    elif 'ABI-L2-MCMIP' in MESSAGE_FROM_PUBSUB['name']:

        abi_l2_mcmip_keys = ["dataset_name",
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

        abi_l2_mcmip_val = [nc.dataset_name,
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

        return (abi_l2_mcmip_keys,abi_l2_mcmip_val)

    # TODO : Update with correct schema 

    elif 'GLM-L2-LCFA' in MESSAGE_FROM_PUBSUB['name']:

        glm_l2_lcfa_keys = ["dataset_name",
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

        glm_l2_lcfa_val = [nc.dataset_name,
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

        return (glm_l2_lcfa_keys,glm_l2_lcfa_val)


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
        'job_name': 'goes16-' + datetime.datetime.now().strftime('%y%m%d-%H%M%S'),
        'project': project,
        'max_num_workers': 3,
        'setup_file': './setup.py',
        'teardown_policy': 'TEARDOWN_ALWAYS',
        'save_main_session': True,
        'streaming': True
   }
   opts = beam.pipeline.PipelineOptions(flags=[], **options)
   p = beam.Pipeline(runner, options=opts)
   (p   | 'events' >> beam.io.ReadFromPubSub('projects/noaa-goes16/topics/demotopic')
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



