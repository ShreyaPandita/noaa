"""Walk through a directory of h5 (netcdf) files, extract attributes.

Walks through directory tree to find h5 (netcdf) files, extract attributes and
enter records into a SQLite3 table.  Finally, extract to a csv.
"""

import os
import sys
import time
import re

from absl import app
from absl import flags
from tensorflow import gfile
# from google3.pyglib import logging
import goes_16_metadata_reader as reader
import tracking_cloudsql as tracking_db
import MySQLdb

FLAGS = flags.FLAGS
flags.DEFINE_boolean('walk_files', False,
                     'Walks through all the files specified and creates '
                     'database entries, queued up to be processed.')
flags.DEFINE_boolean('create_csv', False,
                     'Generates CSV at designated location output_root + '
                     'output_file')
flags.DEFINE_boolean('admin', False,
                     'Enables admin run of the code, for table operations.')
flags.DEFINE_boolean('copy_only', False,
                     ('Testing Mode. Does not process files. Only copy '
                      'from Bigstore.'))
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
flags.DEFINE_multi_string(
    'file_types', ['mcm', 'rad'],
    ('The same purpose as file_type (singlular) but '
     'it needs to match the list of locations. '
     'This is a multi-string Flag, used multiple times .'))
flags.DEFINE_multi_string(
    'locations', ['ABI-L2-MCMIPM/2017/*/*/*.nc', 'ABI-L1b-RadM/2017/*/*/*.nc'],
    ('Subdirectories for Data Locations to Be Walked. '
     'This is a multi-string Flag, used multiple times .'))


def PrintStderr(s):
  """Print to STDERR."""
  print >> sys.stderr, s


def ProcessFile(goes_file, file_type, copy_only=False):
  """Process single file with hdf5 data extraction."""
  # Identified the full path
  # Get the metadata, convert to CSV, return data.
  processor = reader.GOESMetadataReader(file_type, copy_only=copy_only)
  row = processor.GetMetadata(goes_file)
  return (processor.GetCsv(row), row)


def PopulatePathsInDB():
  """Handles the walk through all the files creating entries in the DB.

  Finds each file in the paths specified under FLAGS.locations
  If no entry is found, it creates a path and timestamp only.
  If there is an entry in the DB, it checks the date.
  If file mtime is newer than the DB entry, it processes the new file.
  """
  with tracking_db.TrackingDB() as tdb:
    # Looks for list of locations specified on the command line
    for value in FLAGS.locations:
      file_type = FLAGS.file_types.pop(0)
      location = os.path.join(FLAGS.location_root, value)

      # Walk through all the directories
      file_dict = {}
      file_count = 1

      for goes_file in gfile.Glob(location):
        # TODO(drj@) Add mtime check to update data for newer files.
        goes_file = re.sub('gs:/', '/bigstore', goes_file)
        base_file = os.path.basename(goes_file)
        file_dict[base_file] = goes_file
        file_count += 1
        if file_count % FLAGS.batch_size == 0:
          fetch_paths = ExistsBatch(tdb, file_dict)
          if fetch_paths:
            InsertBatch(tdb, fetch_paths, file_type)
          file_dict = {}
        if FLAGS.max_to_process > 0 and file_count > FLAGS.max_to_process:
          PrintStderr('Over max files. Exiting.')
          exit()
      # catch the remainder files after the modulo
      if file_count % FLAGS.batch_size:
        fetch_paths = ExistsBatch(tdb, file_dict)
        InsertBatch(tdb, fetch_paths, file_type)


def InsertBatch(tdb, path_list, file_type):
  inqueue = 1
  insert_list = []
  data = ''
  for path in path_list:
    mtime = int(time.time())
    insert_list.append((os.path.basename(path), path, data, mtime, file_type,
                        inqueue))
  tdb.insert_many(insert_list)


def ExistsBatch(tdb, file_dict):
  file_list = file_dict.keys()
  results = tdb.entry_exists_many(file_list)
  # results is a list of tuples. Below is unpacking and selection.
  # Builds a dict of results tuple
  fetch_files = [i[0] for i in results]
  # removes elements from file list that were found in the DB
  diff = list(set(file_list).difference(fetch_files))
  # return the list of paths to insert into the DB
  return [file_dict[i] for i in diff]


def main(argv):
  del argv  # Unused.
  processor = reader.GOESMetadataReader()
  PrintStderr('Starting GOES Processing')

  # Run through all the files and put them in the DB for processing.
  if FLAGS.walk_files:
    PrintStderr('Walking GOES Files')
    PopulatePathsInDB()
    PrintStderr('Done Walking GOES Files')
    exit()

  # Creates CSV from the data in the DB.
  if FLAGS.create_csv:
    PrintStderr('Creating GOES CSV Files')
    output_file = os.path.join(FLAGS.output_root, FLAGS.output_file)
    with gfile.Open(output_file, 'w') as csv_file:
      with tracking_db.TrackingDB() as tdb:
        # logging.info('Saving CSV file: %s', output_file)
        tdb.export_csvdata(csv_file, FLAGS.file_type, processor.GetCsvHeader())
    PrintStderr('Done Creating GOES CSV Files')
    exit()

  # Runs admin package for DB
  if FLAGS.admin:
    with tracking_db.TrackingDB() as tdb:
      tdb.admin(processor)
      # logging.info('Running in Admin Mode')
    exit()

  # Run main process
  file_count = FLAGS.max_to_process
  PrintStderr('Processing GOES Files')
  print 'file count: %d '%(file_count)

  while file_count > 0:
    try:
      with tracking_db.TrackingDB() as tdb:
      # Looks for list of locations specified on the command line
        num_bytes = 0
        t0 = float(time.time())
        queue_entries = tdb.get_queue_entry_many(FLAGS.batch_size)
    except MySQLdb.OperationalError as oe:
      PrintStderr(oe)
      raise

    if not queue_entries:
      # logging.info('No new entries found.  Exiting.')
      PrintStderr('No new entries found.  Exiting.')
      exit()

    t1 = float(time.time())
    queue_data = []
    for (filename, path, mtime, file_type) in queue_entries:
      file_count -= 1
      del mtime
      (data, row) = ProcessFile(path, file_type, copy_only=FLAGS.copy_only)
      num_bytes += row['total_size']
      queue_data.append((data, 0, filename))
    t2 = float(time.time())

    if not FLAGS.copy_only:
      # logging.info('Updating Tracking DB Start')
      with tracking_db.TrackingDB() as tdb:
        tdb.update_data_many(queue_data)
        PrintStderr('GOES Processed ' + str(len(queue_data)) + ' files')
    t3 = float(time.time())
    PrintStderr('Files Remaining in Batch: ' + str(file_count))
    PrintStderr('Time to Query List: ' + str(t1 - t0))
    PrintStderr('Time to Process List: ' + str(t2 - t1))
    PrintStderr('Time to Update List: ' + str(t3 - t2))
  PrintStderr('Done Processing GOES Files')


if __name__ == '__main__':
  app.run(main)
