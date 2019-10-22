"""A class for reading NetCDF metadata and writes them to a csv file."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import csv
import re
import StringIO
import tempfile
import h5py
from google3.pyglib import flags
from google3.pyglib import gfile

FLAGS = flags.FLAGS

flags.DEFINE_string('file_type', 'rad',
                    'Corresponds to the type of file being extracted.')


class NetCdfMetadataReader(object):
  """A class for reading NetCDF metadata and writes them to a csv file."""

  def __init__(self, config_disc, file_type=None):
    """Constructor.

    Args:
      config_disc: Dictionary containing metadata mappings.
      file_type: Type of NetCDF file being parsed.
    """
    if not file_type:
      self._file_type = FLAGS.file_type
    else:
      self._file_type = file_type

    self._metadata_disc = config_disc[self._file_type]

    self._metadata_keys = {}
    self._data_keys = {}

    if self._metadata_disc.get('metadata') is not None:
      self._metadata_keys = self._metadata_disc['metadata']

    if self._metadata_disc.get('data') is not None:
      self._data_keys = self._metadata_disc['data']

    self._fieldnames = []
    self._fieldnames.extend(self._GetFieldNames(self._metadata_keys))
    self._fieldnames.extend(self._GetFieldNames(self._data_keys))
    self._fieldnames.extend(['base_url'])
    self._fieldnames.sort()

  def _GetFieldNames(self, data):
    """Reads dictionary and returns header column names.

    Args:
      data: Dictionary containing header mapping.

    Returns:
      Array containing header column names.
    """
    fieldnames = []
    if data:
      for outer_k, outer_v in data.items():
        del outer_k
        if isinstance(outer_v, unicode):
          fieldnames.append(outer_v)
        else:
          for inner_k, inner_v in outer_v.items():
            del inner_k
            fieldnames.append(inner_v)

    return fieldnames

  def _FixThePath(self, file_path):
    """Switches cloud storage path prefix.

    Args:
      file_path: Cloud storage path with bigstore prefix.

    Returns:
      String with gs prefix.
    """
    return re.sub('/bigstore', 'gs:/', file_path)

  def GetMetadata(self, file_path):
    """Reads input file for metadata.

    Args:
      file_path: Cloud storage path to NetCDF file.

    Returns:
      Dictinary of containing select metadata.
    """
    _, temp_file = tempfile.mkstemp()
    gfile.Copy(file_path, temp_file, True)
    md_dict = {}

    with h5py.File(temp_file, 'r') as nc_file:
      # Metadata parse
      for k, v in self._metadata_keys.items():
        if nc_file.attrs.get(k) is not None:
          md_dict.update({v: nc_file.attrs[k].item()})

      # Data parse
      for outer_k, outer_v in self._data_keys.items():
        if nc_file.get(outer_k) is not None:
          if isinstance(outer_v, unicode):
            md_dict.update({outer_v: nc_file[outer_k].value})
          else:
            for inner_k, inner_v in outer_v.items():
              if nc_file[outer_k].attrs.get(inner_k) is not None:
                md_dict.update({
                    inner_v: nc_file[outer_k].attrs[inner_k].item()
                })

      base_url = self._FixThePath(file_path)
      md_dict.update({'base_url': base_url})
    gfile.Remove(temp_file)
    return md_dict

  def GetCsv(self, metadata):
    """Converts metadata into csv row.

    Args:
      metadata: Dictinary of containing select metadata.

    Returns:
      String formatted as a csv.
    """
    output = StringIO.StringIO()
    tmpwriter = csv.DictWriter(
        output, fieldnames=self._fieldnames, lineterminator='')
    tmpwriter.writerow(metadata)
    returnval = output.getvalue()
    output.close()
    return returnval

  def GetCsvHeader(self):
    """Returns csv header for the metadata.

    Returns:
      String formatted as a csv.
    """
    output = StringIO.StringIO()
    tmpwriter = csv.DictWriter(
        output, fieldnames=self._fieldnames, lineterminator='')
    tmpwriter.writeheader()
    returnval = output.getvalue()
    output.close()
    return returnval
