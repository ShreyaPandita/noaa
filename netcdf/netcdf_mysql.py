"""Sets up DB structure and methods to allow tracking of processed files."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import google3
import MySQLdb

from google3.pyglib import app
from google3.pyglib import flags
from google3.pyglib import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('instance',
                    '/cloudsql/google.com:cicentcom:us-central1:gtech-feeds',
                    'Cloud SQL instance Name')
flags.DEFINE_string('user', 'cianalysismysql',
                    'Username for Cloud SQL instance.')
# MySQL root password here:
# https://valentine.corp.google.com/#/show/1507228762951889?tab=metadata
flags.DEFINE_string('password', '', 'Password to use for Cloud SQL instance.')

flags.DEFINE_string('database', 'CpdNetCdf',
                    'Name of the Cloud SQL database to use for Cloud '
                    'SQL instance.')

flags.DEFINE_integer('connect_retries', 5,
                     'Max number of DB connect retries before failing.')

flags.DEFINE_string('table_name', 'netcdf_metadata',
                    'Name of the Cloud SQL table to use for Cloud')

flags.DEFINE_string('project_name', 'Test',
                    'Name of the Cloud SQL table to use for Cloud')


class TrackingDB(object):
  """Class to connect and manage DB of filenames already processed."""

  def __init__(self):
    """Constructor."""
    if not FLAGS.instance:
      raise app.UsageError('--instance must be specified')
    unix_socket = FLAGS.instance
    self._table_name = FLAGS.table_name
    self._project_name = FLAGS.project_name
    # Connection retries mitigate a low frequency of connect errors,
    # causing global workflow failures.
    retry_count = FLAGS.connect_retries
    while retry_count:
      retry_count -= 1
      try:
        self._conn = MySQLdb.connect(unix_socket=unix_socket, user=FLAGS.user,
                                     passwd=FLAGS.password, db=FLAGS.database,
                                     connect_timeout=1000)
      except MySQLdb.OperationalError as oe:
        if not retry_count:
          logging.info(oe)
          logging.info('Multiple fail to connect to DB. Aborting.')
          raise
        else:
          logging.info('Failed to connect to DB. Retrying (20s.')
          time.sleep(20)
      else:
        break

    self._cursor = self._conn.cursor()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    pass

  def GetQueueEntryMany(self, batch_size):
    """Selects unprocessed files and marks them being reviewed.

    Args:
      batch_size: Number of unprocessed files to return.

    Returns:
      List containing data about the unprocessed files.
    """
    self._conn.autocommit(False)
    queue_entry = ('SELECT filename, path, mtime, file_type FROM %s '
                   'WHERE inqueue=1 AND project = "%s" limit %s for update'
                   % (self._table_name, self._project_name, batch_size))
    self._cursor.execute(queue_entry)
    queue_entries = self._cursor.fetchall()
    queue_update = []
    for (filename, path, mtime, file_type) in queue_entries:
      del mtime
      del path
      del file_type
      queue_update.append(('', 2, filename))
    self.UpdateDataMany(queue_update)
    self._conn.commit()
    return queue_entries

  def EntryExistsMany(self, filename_list):
    """Selects whether or not files exist in the database.

    Args:
      filename_list: List of file names (with path).

    Returns:
      List of files that already exist in the database.
    """
    file_list = ','.join(['"' + f + '"' for f in filename_list])
    search_entry = ('SELECT  path, filename FROM %s '
                    'WHERE path in (%s) AND project= "%s"'
                    % (self._table_name, file_list, self._project_name))
    self._cursor.execute(search_entry)
    return self._cursor.fetchall()

  def InsertMany(self, item_tuples):
    """Inserts a list of new rows into database.

    Args:
      item_tuples: List of tuples containing row data.
    """
    insert = ('INSERT INTO '+self._table_name+' VALUES '
              '("'+self._project_name+'",%s,%s,%s,%s,%s,%s)')
    self._cursor.executemany(insert, item_tuples)
    self._conn.commit()

  def UpdateDataMany(self, queue_data):
    """Updates the data and inqueue field for many rows.

    Args:
      queue_data: List of rows to be updated with update values.
    """
    update = ('UPDATE '+self._table_name+' SET data=%s, inqueue=%s '
              'WHERE filename=%s AND project="'+self._project_name+'"')
    self._cursor.executemany(update, queue_data)
    self._conn.commit()

  # found in code
  def ExportCsvData(self, csv_file, file_type, batch_size=100000):
    """Exports collected metadata to csv file.

    Args:
      csv_file: File buffer to write to (GFile).

      file_type: Type of metadata being written to csv.

      batch_size: Number of lines written to csv per select.
    """
    limit = 0
    while True:
      export = ('SELECT data FROM %s '
                'WHERE file_type="%s" and data > "" '
                'AND project= "%s" ORDER BY mtime '
                'LIMIT %s, %s'
                % (self._table_name, file_type, self._project_name,
                   limit, batch_size))
      self._cursor.execute(export)
      if not self._cursor.rowcount:
        break

      for row in self._cursor:
        csv_file.write(row[0] + '\n')
      limit += batch_size
