def plot_image(ncfilename):
    import logging
    import numpy as np
    from netCDF4 import Dataset
    import csv
    import tempfile
    import os.path

    tempdir = tempfile.mkdtemp()
    csvfile = os.path.join(tempdir, os.path.basename('test-123.csv'))
    with Dataset(ncfilename, 'r') as nc:
        #print nc.variables.keys()
        #print nc.variables["y_image_bounds"][:]
        with open(csvfile, 'w') as fw:
            writer = csv.writer(fw)
            writer.writerow(nc.variables.keys())
            #for k in nc.variables.keys():
            #   print nc.variables[k][:]
            rad = nc.variables['mean_radiance_value_of_valid_pixels'][:]
            print rad
            print csvfile
    return None

plot_image("test.nc")






