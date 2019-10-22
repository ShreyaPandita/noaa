"""TODO(jeremyschanz): DO NOT SUBMIT without one-line documentation for executor.

TODO(jeremyschanz): DO NOT SUBMIT without a detailed description of executor.
"""
import logging
import os
import sys

from cloud_handler import CloudLoggingHandler
from cron_executor import Executor

PROJECT = 'google.com:cicentcom'  # change this to match your project
TOPIC = 'goes16-process'

script_path = os.path.abspath(os.path.join(os.getcwd(),
                                           'goes.py'))
args = '--batch_size=1000 --database=GOES17 --max_to_process=500000'

sample_task = "python -u %s %s" % (script_path, args)


root_logger = logging.getLogger('cron_executor')
root_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)

cloud_handler = CloudLoggingHandler(on_gce=True, logname="task_runner")
root_logger.addHandler(cloud_handler)

# create the executor that watches the topic, and will run the job task
executor = Executor(topic=TOPIC, project=PROJECT, task_cmd=sample_task, subname='Goes16_Process_task')

# add a cloud logging handler and stderr logging handler
job_cloud_handler = CloudLoggingHandler(on_gce=True,
                                        logname=executor.subname)
executor.job_log.addHandler(job_cloud_handler)
executor.job_log.addHandler(ch)
executor.job_log.setLevel(logging.DEBUG)


# watches indefinitely
executor.watch_topic()
