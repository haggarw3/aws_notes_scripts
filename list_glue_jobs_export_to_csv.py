import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



import boto3
client = boto3.client('glue')
response = client.get_jobs(MaxResults=500)

response.keys()
job_list = response['Jobs']
job_list[0]
job_list[0]['Name']


job_names = []
for job in job_list:
    job_names.append(job['Name'])

import pandas as pd
df = pd.DataFrame(job_names)

# df.to_csv('job_names.csv')  # did not work - do not know where the output is stored 


# Export to a csv –
# Not first create the S3 bucket 

from io import StringIO # python3; python2: BytesIO 
bucket = 'glue-jobs-list' # already created on S3
csv_buffer = StringIO()
df.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())
job.commit()

