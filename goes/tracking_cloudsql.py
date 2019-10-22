"""Sets up DB structure and methods to allow tracking of processed files.

There are two modes: processing and admin.  The --admin flag allows the
following commances to be run:

  --add_entry: Adds a record to the table.Expects filename,path,data,mtime list
    (a comma separated list)
  --[no]create_table: Creates the table. Normally this already exists.
    (default: 'false')
  --delete_entry: deletes a record in the table. Expects "filename"
  --[no]drop_table: Drops the filename table.
    (default: 'false')
  --[no]dump_table: Dumps table Contents.
    (default: 'false')
  --[no]export_csv: Exports to Named CSV file the records of named file_type.
    (default: 'false')
  --get_entry: Gets a record from the table. Expects "filename"
  --update_entry: Updates a record in the table. Expects
    filename,path,data,mtime list.Key is filename.
    (a comma separated list)

The DB schema is very simple:

TABLE goes_metadata
   filename text - this is the filename, used as the unique key
   path text - the full path to the file.  Not currently used, but poputlated
   data text - the CSV record of the metadata extracted from the files
   mtime integer - the epoch modification time of the file. New files are
                   reprocessed
   file_type text - indicates the type of file processed.  Keys are defined in
                   goes_16_metadata_reader.py.  Currently one of "cmi", "mcm",
                   "rad".
"""
import sys
import time
import MySQLdb

from absl import app
from absl import flags
from tensorflow import gfile
# from google3.pyglib import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('instance', 'google.com:cicentcom:us-central1:gtech-feeds',
                    'Cloud SQL instance Name')
flags.DEFINE_string('user', 'root',
                    'Username for Cloud SQL instance.')
# MySQL root password here:
# https://valentine.corp.google.com/#/show/1507228762951889?tab=metadata
flags.DEFINE_string('password', 'dzqf3fmGHKIAqhrn', 'Password to use for Cloud SQL instance.')
flags.DEFINE_string('database', 'GOES16',
                    'Name of the Cloud SQL database to use for Cloud '
                    'SQL instance.')
flags.DEFINE_boolean('get_queue_many', False,
                     'Tests getting members from the queue.')
flags.DEFINE_boolean('drop_table', False,
                     'Drops the filename table.')
flags.DEFINE_boolean('create_table', False,
                     'Creates the table. Normally this already exists.')
flags.DEFINE_boolean('dump_table', False, 'Dumps table Contents.')
flags.DEFINE_boolean('get_queue_entry', False, 'Gets and item from Queue.')
flags.DEFINE_list('add_entry', None,
                  'Adds a record to the table.'
                  'Expects filename,path,data,mtime list')
flags.DEFINE_list('update_entry', None,
                  'Updates a record in the table. '
                  'Expects filename,path,data,mtime list.'
                  'Key is filename.')
flags.DEFINE_string('get_entry', None,
                    'Gets a record from the table. Expects "filename"')
flags.DEFINE_string('delete_entry', None,
                    'deletes a record in the table. Expects "filename"')
flags.DEFINE_string('add_to_queue', None,
                    'Updates inqueue bit to 1 for filename. '
                    'Expects filename. Key is filename.')
flags.DEFINE_string('remove_from_queue', None,
                    'Updates inqueue bit to 0 for filename. '
                    'Expects filename. Key is filename.')
flags.DEFINE_string('export_csv', None,
                    'Exports CSV file to path "export_csv=path".')
flags.DEFINE_integer('connect_retries', 5,
                     'Max number of DB connect retries before failing.')


class TrackingDB(object):
  """Stuff to connect and manage DB of filenames already processed."""

  def __init__(self):
    if not FLAGS.instance:
      raise app.UsageError('--instance must be specified')
    unix_socket = '/cloudsql/%s' % FLAGS.instance

    # Connection retries mitigate a low frequency of connect errors,
    # causing global workflow failures.
    retry_count = FLAGS.connect_retries
    while retry_count:
      retry_count -= 1
      try:
        self._conn = MySQLdb.connect(host='35.202.123.120', user=FLAGS.user,
                                     passwd=FLAGS.password, db=FLAGS.database,
                                     connect_timeout=1000)
      except MySQLdb.OperationalError as oe:
        if not retry_count:
          # logging.info(oe)
          # logging.info('Multiple fail to connect to DB. Aborting.')
          raise
        else:
          # logging.info('Failed to connect to DB. Retrying (20s.')
          time.sleep(20)
      else:
        break

    self._cursor = self._conn.cursor()

  def admin(self, processor):
    """Allows some basic table wrangling, create, delete, etc."""
    if FLAGS.get_queue_entry:
      self._get_queue_entry()
    if FLAGS.drop_table:
      self._drop_table()
    if FLAGS.create_table:
      self._create_table()
    if FLAGS.dump_table:
      self._dump_table()
    if FLAGS.get_entry:
      self._get_entry()
    if FLAGS.add_entry:
      self._add_entry()
    if FLAGS.delete_entry:
      self._delete_entry()
    if FLAGS.update_entry:
      self._update_entry()
    if FLAGS.export_csv:
      self._export_csvdata(processor)
    if FLAGS.add_to_queue:
      self._add_to_queue(FLAGS.add_to_queue)
    if FLAGS.remove_from_queue:
      self._remove_from_queue(FLAGS.remove_from_queue)
    if FLAGS.get_queue_many:
      self._get_queue_many()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    pass

  def _test_table_exists(self):
    test_select = ('SELECT name FROM sqlite_master WHERE type="table" AND '
                   'name="goes_metadata"')
    self._cursor.execute(test_select)
    if self._cursor.fetchone():
      return True
    else:
      return False

  def _get_entry(self, filename=None):
    if not filename:
      filename = FLAGS.get_entry
    print self.retrieve_entry(filename)

  def _get_queue_entry(self):
    print self.get_queue_entry()

  def get_queue_entry(self):
    queue_entry = ('SELECT filename, path, mtime, file_type FROM goes_metadata '
                   'WHERE inqueue=1 order by rand() limit 1')
    self._cursor.execute(queue_entry)
    return self._cursor.fetchone()

  def _get_queue_many(self):
    print self.get_queue_entry_many(FLAGS.batch_size)

  def get_queue_entry_many(self, batch_size):
    self._conn.autocommit(False)
    queue_entry = ('SELECT filename, path, mtime, file_type FROM goes_metadata '
                   'WHERE inqueue=1 order by mtime desc limit %s'
                   % (batch_size))
    self._cursor.execute(queue_entry)
    queue_entries = self._cursor.fetchall()
    queue_update = []
    for (filename, path, mtime, file_type) in queue_entries:
      del mtime
      del path
      del file_type
      queue_update.append(('', 2, filename))
    self.update_data_many(queue_update)
    self._conn.commit()
    return queue_entries

  def entry_exists(self, filename):
    search_entry = ('SELECT * FROM goes_metadata WHERE filename="%s"'
                    % (filename))
    self._cursor.execute(search_entry)
    if self._cursor.fetchone():
      return True
    else:
      return False

  def entry_exists_many(self, filename_list):
    file_list = ','.join(['"' + f + '"' for f in filename_list])
    select = 'SELECT filename, path FROM goes_metadata WHERE filename in ({0})'
    search_entry = select.format(file_list)
    try:
      self._cursor.execute(search_entry)
      return self._cursor.fetchall()
    except:
      print >> sys.stderr, search_entry
      raise

  def retrieve_entry(self, filename):
    search_entry = ('SELECT * FROM goes_metadata WHERE filename="%s"'
                    % (filename))
    self._cursor.execute(search_entry)
    return self._cursor.fetchone()

  def _drop_table(self):
    drop = ('DROP TABLE if exists goes_metadata')
    self._cursor.execute(drop)

  def _create_table(self):
    create = ('CREATE TABLE goes_metadata (filename text, path text,'
              'data text, mtime integer, file_type text, '
              'inqueue tinyint(1) default 0)')
    self._cursor.execute(create)

  def _add_entry(self, filename=None, path=None, data=None, mtime=None,
                 inqueue=None):
    if not filename:
      (filename, path, data, mtime, file_type, inqueue) = FLAGS.add_entry
    self.insert_entry(filename, path, data, mtime, file_type, inqueue)

  def insert_entry(self, filename, path, data, mtime, file_type, inqueue):
    insert = ('INSERT INTO goes_metadata VALUES '
              '("%s","%s","%s","%s","%s", "%s")'%
              (filename, path, data, mtime, file_type, inqueue))
    self._cursor.execute(insert)

  def insert_many(self, item_tuple):
    insert = ('INSERT INTO goes_metadata VALUES '
              '(%s,%s,%s,%s,%s,%s)')
    self._cursor.executemany(insert, item_tuple)

  def _update_entry(self, filename=None, path=None, data=None, mtime=None,
                    inqueue=None):
    if not filename:
      (filename, path, data, mtime, file_type, inqueue) = FLAGS.update_entry
    self.update_entry(filename, path, data, mtime, file_type, inqueue)

  def update_entry(self, filename, path, data, mtime, file_type, inqueue):
    update = (('UPDATE goes_metadata '
               'SET path="%s",data="%s",mtime=%s,file_type="%s, inqueue=%s" '
               'WHERE filename="%s"')%
              (path, data, mtime, file_type, inqueue, filename))
    self._cursor.execute(update)
    self._conn.commit()

  def update_data(self, filename, data, inqueue=0):
    update = (('UPDATE goes_metadata '
               'SET data="%s", inqueue=%s '
               'WHERE filename="%s"') %
              (data, inqueue, filename))
    self._cursor.execute(update)
    self._conn.commit()

  def update_data_many(self, queue_data):
    update = ('UPDATE goes_metadata SET data=%s, inqueue=%s '
              'WHERE filename=%s')
    self._cursor.executemany(update, queue_data)
    self._conn.commit()

  def _add_to_queue(self, filename=None):
    if not filename:
      raise app.UsageError('Need to specify filename to add to queue.')
    else:
      self.update_inqueue(filename, 1)

  def _remove_from_queue(self, filename=None):
    if not filename:
      raise app.UsageError('Need to specify filename to remove from queue.')
    else:
      self.update_inqueue(filename, 0)

  def update_inqueue(self, filename, inqueue):
    update = (('UPDATE goes_metadata SET inqueue=%s WHERE filename="%s"')%
              (inqueue, filename))
    self._cursor.execute(update)
    self._conn.commit()

  def _delete_entry(self):
    filename = FLAGS.delete_entry
    delete = ('DELETE FROM goes_metadata WHERE filename="%s"'% (filename))
    self._cursor.execute(delete)

  def _dump_table(self):
    dump = ('SELECT * FROM goes_metadata')
    self._cursor.execute(dump)
    for row in self._cursor:
      print row

  def _export_csvdata(self, processor):
    csvfile = gfile.Open(FLAGS.export_csv, 'w')
    self.export_csvdata(csvfile,
                        FLAGS.file_type,
                        processor.GetCsvHeader()
                       )

  def export_csvdata(self, csvfile, file_type, header):
    export = ('SELECT data FROM goes_metadata '
              'WHERE file_type="%s" and data > ""' % (file_type))
    self._cursor.execute(export)
    csvfile.write(header + '\n')
    for row in self._cursor:
      csvfile.write(row[0] + '\n')
