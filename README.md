# NOAA data pipeline architecture
![alt text](https://storage.googleapis.com/goes-arch/goes16.png)


# Accessing GOES-16 data from your GCP envrironment 
 
 This repository contains code to setup a streaming pipeline for transfering GOES-16 data to your local Bigquery project. 

1. Set up the pipeline environment :
	Create a Compute Engine instance from the GCP console
	#Git clone this repository on that machine and install the necessary software (this takes a while):
	sudo apt-get install git
	git clone https://github.com/ShreyaPandita/noaa.git
	cd /noaa
	# Run the install.sh script which installs local dependencies like numpy, netCDF4 etc. 
	bash install.sh


Note : This code was tested on: 
Python 2.7
Ubuntu 18.x 

2. Link to NOAA public storage bucket 
https://pantheon.corp.google.com/storage/browser/gcp-public-data-goes-16


# For setting up a test pipeline 

1. Create a test topic 
gcloud pubsub topics create demotopic 

2. Refer pipeline Dataflow runner code in noaa_pipeline_test.py - beam.io.ReadFromPubSub subscribes to the test topic you created 

3. Modify the code to match your topic name 

4. Create a empty test dataset in your BigQuery project

5. Run the test pipeline 
python noaa_pipeline_test.py --bucket noaa-goes16-dummypipeline --project $(gcloud config get-value project)

6. Publish test messages 

7. To test the pipeline flow - publish test notification messages to this topic from console or gcloud APIs ( refer testnotification.txt for samples )

5. Verify that the nc file is converted to .csv in uploded in your storage bucket, and eventually table with NOAA data gets created in your BigQuery test dataset 


# For setting up a staging pipeline 
1. Create a subscription to the public NOAA topic
gcloud beta pubsub subscriptions create testsubscription \
    --topic=projects/gcp-public-data---goes-16/topics/gcp-public-data-goes-16

2. Create a empty staging dataset in your BigQuery project

3. Run the staging pipeline 
python noaa_pipeline_staging.py --bucket noaa-goes16-dummypipeline --project $(gcloud config get-value project)

4. Monitor Job Metrics from dataflow console to monitor the elements/sec being added in your pipeline

5. Verify that the nc files are being converted to .csv in uploded in your storage bucket, and eventually tables with NOAA data gets created in your BigQuery staging dataset 

6. Please refer to https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#streaming-autoscaling to set up autoscaling and other tuning parameter for your streaming pipeline so that streaming execution is effective 



