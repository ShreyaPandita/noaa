"""Walk through a directory of h5 (netcdf) files, extract attributes.

Walks through directory tree to find h5 (netcdf) files, extract attributes and
enter records into a MySql table.  Finally, extract to a csv.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import json
import os
import sys
from google3.experimental.users.jeremyschanz.netCdfPlugin.plugins import netcdf_metadata_reader as reader
from google3.experimental.users.jeremyschanz.netCdfPlugin.plugins import netcdf_mysql as tracking_db
from google3.pyglib import app
from google3.pyglib import flags
from google3.pyglib import gfile

FLAGS = flags.FLAGS
flags.DEFINE_boolean('walk_files', False,
                     'Walks through all the files specified and creates '
                     'database entries, queued up to be processed.')
flags.DEFINE_boolean('create_csv', False,
                     'Generates CSV at designated location output_root + '
                     'output_file')
flags.DEFINE_string('output_root', ('/bigstore/commerce-channels-gtech-feeds'
                                    '/Axon Pilots/cloud/noaa/goes16/'),
                    'Root location of the generated CSV files.')
flags.DEFINE_string('output_file', 'abi_l2_cmip.csv',
                    'Name of CSV file being generated.')
flags.DEFINE_integer('batch_size', 50,
                     'How many files to process in a single SQL call.')
flags.DEFINE_integer('max_to_process', -1,
                     'How many files to process before exit. -1 == do all.')
flags.DEFINE_string('location_root', '/bigstore/gcp-public-data-goes-16/',
                    'Root location of the files to be processed.')
flags.DEFINE_string('config_file_path', None,
                    'Path to config file for NetCdf metadata.')

flags.DEFINE_multi_string(
    'file_types', ['mcm', 'rad'],
    ('The same purpose as file_type (singlular) but '
     'it needs to match the list of locations. '
     'This is a multi-string Flag, used multiple times .'))
flags.DEFINE_multi_string(
    'locations', ['ABI-L2-MCMIPM/2017/*/*/*.nc', 'ABI-L1b-RadM/2017/*/*/*.nc'],
    ('Subdirectories for Data Locations to Be Walked. '
     'This is a multi-string Flag, used multiple times .'))


def _PrintStderr(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)


def _ProcessNetCdfFile(config_dict, netcdf_file, file_type):
  """Process single file calling NetCdf Reader.

  Args:
    config_dict: Config file dict.
    netcdf_file: Path to NetCDF file.
    file_type: Type of NetCDF file.

  Returns:
    CSV string of the proccesed metadata data.
  """
  processor = reader.NetCdfMetadataReader(config_dict, file_type)
  row = processor.GetMetadata(netcdf_file)
  return processor.GetCsv(row)


def _CreateCsv(config_dict, tdb, output_root, output_file, file_type,
               batch_size):
  """Writes data from db to CSV based on file type.

  Args:
    config_dict: Config file dict.
    tdb: Instance of TrackingDB.
    output_root: Path to directory of output CSV.
    output_file: Name of output CSV file.
    file_type: Type of NetCDF file.
    batch_size: Number of rows written to CSV at once.
  """
  output_file = os.path.join(output_root, output_file)
  processor = reader.NetCdfMetadataReader(config_dict, file_type)
  with gfile.Open(output_file, 'w') as csv_file:
    _PrintStderr('Saving CSV file: %s', output_file)
    csv_file.write(processor.GetCsvHeader() + '\n')
    tdb.ExportCsvData(csv_file, file_type,
                      batch_size)


def _WalkFiles(tdb, locations, file_types, location_root, batch_size):
  """Handles the walk through all the files creating entries in the DB.

  Args:
    tdb: Instance of TrackingDB.
    locations: An array containing the file paths to be walked.
    file_types: An array containing the file types being walked.
    location_root: Path to base directory where files are being walked.
    batch_size: Number of rows written db at once.
  """
  for value in locations:
    file_type = file_types.pop(0)
    location = os.path.join(location_root, value)

    file_list = []
    file_count = 1

    for file_path in gfile.Glob(location):
      file_list.append(file_path)
      file_count += 1
      if file_count % batch_size == 0:
        fetch_paths = _ExistsBatch(tdb, file_list)
        if fetch_paths:
          _InsertBatch(tdb, fetch_paths, file_type)
        file_list = []

    # catch the remainder files after the modulo
    if file_count % batch_size:
      fetch_paths = _ExistsBatch(tdb, file_list)
      _InsertBatch(tdb, fetch_paths, file_type)


def _InsertBatch(tdb, path_list, file_type):
  """Batch inserts new files to db.

  Args:
    tdb: Instance of TrackingDB.
    path_list: List of file paths being added to db.
    file_type: Type of NetCDF file.
  """
  inqueue = 1
  insert_list = []
  data = ''
  for path in path_list:
    mtime = gfile.Stat(path).mtime
    insert_list.append((os.path.basename(path), path, data, mtime, file_type,
                        inqueue))
  tdb.InsertMany(insert_list)


def _ExistsBatch(tdb, file_list):
  """Checks if the files be walked are already in the db.

  Args:
    tdb: Instance of TrackingDB.
    file_list: List of file paths being checked.

  Returns:
    Returns list of file paths that are not in the db.
  """
  results = tdb.EntryExistsMany(file_list)
  fetch_files = [i[0] for i in results]
  diff = list(set(file_list).difference(fetch_files))
  return diff


def _ProcessFiles(config_dict, max_to_process, batch_size):
  """Checks if the files be walked are already in the db.

  Args:
    config_dict: Config file dict.
    max_to_process: Max number of files to proccess.
    batch_size: Number of rows written db at once.
  """
  file_count = max_to_process
  while file_count > 0:

    with tracking_db.TrackingDB() as tdb:
      queue_entries = tdb.GetQueueEntryMany(batch_size)

    if not queue_entries:
      _PrintStderr('No new entries found.  Exiting.')
      return

    queue_data = []
    for (filename, path, mtime, file_type) in queue_entries:
      file_count -= 1
      del mtime
      data = _ProcessNetCdfFile(config_dict, path, file_type)
      queue_data.append((data, 0, filename))

    with tracking_db.TrackingDB() as tdb:
      tdb.UpdateDataMany(queue_data)
      _PrintStderr('NetCdf Processed ' + str(len(queue_data)) + ' files')
    _PrintStderr('Files Remaining in Batch: ' + str(file_count))


def _LoadConfigFile(config_path):
  """Converts Json config file into a dict.

  Args:
    config_path: Path to config file.

  Returns:
    Returns dict representation of the config file.
  """
  with gfile.GFile(config_path, 'r') as input_file:
    config_data = json.load(input_file)
    return config_data


def main(_):
  if FLAGS.walk_files:
    _PrintStderr('Walking NetCDF Files')
    with tracking_db.TrackingDB() as tdb:
      _WalkFiles(tdb, FLAGS.locations, FLAGS.file_types, FLAGS.location_root,
                 FLAGS.batch_size)
    _PrintStderr('Done Walking NetCDF Files')
  elif FLAGS.create_csv:
    _PrintStderr('Creating NetCDF CSV Files')
    config_dict = _LoadConfigFile(FLAGS.config_file_path)
    with tracking_db.TrackingDB() as tdb:
      _CreateCsv(config_dict, tdb, FLAGS.output_root,
                 FLAGS.output_file, FLAGS.file_type, FLAGS.batch_size)
    _PrintStderr('Done Creating NetCDF CSV Files')
  else:
    _PrintStderr('Processing NetCdf Files')
    config_dict = _LoadConfigFile(FLAGS.config_file_path)
    _ProcessFiles(config_dict, FLAGS.max_to_process,
                  FLAGS.batch_size)
    _PrintStderr('Done Processing NetCdf Files')


if __name__ == '__main__':
  app.run(main)
