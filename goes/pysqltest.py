"""TODO(jeremyschanz): DO NOT SUBMIT without one-line documentation for pysqltest.

TODO(jeremyschanz): DO NOT SUBMIT without a detailed description of pysqltest.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import MySQLdb
import time
import sys
import os
import re
import tempfile
from absl import app
from absl import flags
from google3.pyglib import gfile
#from concurrent import futures
#from multiprocessing.pool import ThreadPool

import json


FLAGS = flags.FLAGS

flags.DEFINE_string('instance', 'google.com:cicentcom:us-central1:gtech-feeds',
                    'Cloud SQL instance Name')
flags.DEFINE_string('user', 'root',
                    'Username for Cloud SQL instance.')
# MySQL root password here:
# https://valentine.corp.google.com/#/show/1507228762951889?tab=metadata
flags.DEFINE_string('password', 'dzqf3fmGHKIAqhrn', 'Password to use for Cloud SQL instance.')
flags.DEFINE_string('database', 'GOES17',
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

def main2(_):

  batch_size = 10
  unix_socket = '/cloudsql/google.com:cicentcom:us-central1:gtech-feeds'
  con = MySQLdb.connect(unix_socket=unix_socket, user=FLAGS.user, passwd=FLAGS.password, db=FLAGS.database, connect_timeout=1000)

  queue_entry = ('SELECT filename, path, mtime, file_type FROM goes_metadata '
                 'WHERE inqueue=1 limit %s'
                 % (batch_size))
  with con:
    t1 = float(time.time())
    cur = con.cursor()
    cur.execute(queue_entry)

    rows = cur.fetchall()
    t2 = float(time.time())
    print('Select: ' + str(t2 - t1))
    queue_update = []
    queue_path = []
    for (filename, path, mtime, file_type) in rows:
      del mtime
      del file_type
      queue_path.append(path)
      queue_update.append((1, filename))

    update = ('UPDATE goes_metadata SET inqueue=%s '
              'WHERE filename=%s')
    cur.executemany(update, queue_update)
    t3 = float(time.time())
    print('Update: ' + str(t3 - t2))

    start_move = time.time()
    temp_folder = tempfile.mkdtemp()
    _, temp_file = tempfile.mkstemp()
    # queue_path.sort(key=lambda tup: tup[0])

    def load_url(path):
      file_name = os.path.basename(path)
      temp_file_path = os.path.join(temp_folder, file_name)
      gfile.Copy(path, temp_file_path, True)
      return path
    '''
    # We can use a with statement to ensure threads are cleaned up promptly
    with futures.ThreadPoolExecutor(max_workers=20) as executor:
      # Start the load operations and mark each future with its URL
      future_to_url = {
          executor.submit(load_url, url): url for url in queue_path}
      for fut in futures.as_completed(future_to_url):
        url = future_to_url[fut]
        try:
          data = fut.result()
        except Exception as exc:
          print('%r generated an exception: %s' % (url, exc))
    '''


    results = ThreadPool(8).imap_unordered(load_url, queue_path)
    for path in results:
      print(path)

    middle_move = time.time()
    print('download ind: ' + str(middle_move - start_move))

    for path in queue_path:
      gfile.Copy(path, temp_file, True)
      gfile.Remove(temp_file)



    print(gfile.ListDir(temp_folder))
    gfile.DeleteRecursively(temp_folder)
    end_move = time.time()
    print('download: ' + str(end_move - middle_move))


path = '/bigstore/cloud_public_data/test.json'

def main(_):
  with gfile.GFile(path, 'r') as input_file:
    marker_data = json.load(input_file)

  for k, v in marker_data["rad"]["metadata"].items():
    print(k, v)




if __name__ == '__main__':
  app.run(main)
