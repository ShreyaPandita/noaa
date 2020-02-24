def plot_image(ncfilename):
    import logging
    import numpy as np
    from netCDF4 import Dataset
    import csv
    
    with Dataset(ncfilename, 'r') as nc:
        #print nc.variables.keys()
        print nc.variables["y_image_bounds"][:]
        with open('test.csv', 'w') as fw:
            writer = csv.writer(fw)
            writer.writerow(nc.variables.keys())
            for k in nc.variables.keys():
                print nc.variables[k][:]
        #rad = nc.variables['Rad'][:]
        #print rad
    return None

plot_image("test.nc")






