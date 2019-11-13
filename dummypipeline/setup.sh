#################################################################################################
# Setup file - This will setiup the environment for kicking off the NOAA data pipeline 
#
#################################################################################################


#!/bin/bash

trigger_bucket=noaa_test_bucket


# Deploy the cloud function 
#deploycmd=$(gcloud functions deploy hello_gcs_generic --runtime python37 --trigger-resource \
#				 $trigger_bucket --trigger-event google.storage.object.finalize)

# Trigger the cloud function 
for i in {0..1}
do
  gsutil cp gcf-test"$i".txt gs://$trigger_bucket
  gcloud functions logs read --limit 100
done
