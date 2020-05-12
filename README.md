# NOAA DATA PIPELINE ARCHITECTURE
![alt text](https://storage.googleapis.com/goes-arch/goes16.png)


# Environment setup 
Run the install.sh script which installs local dependencies like numpy, netCDF4 etc. 
Python 2.7
Ubuntu 18.x 

# Run the pipeline 
python noaa_pipeline.py --bucket noaa-goes16-dummypipeline --project $(gcloud config get-value project)






