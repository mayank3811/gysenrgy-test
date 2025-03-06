# load data from bucket to bigquery
# to load the data I have used Dataproc with spark. 
# here i have created a loop in which all the files are read one at a time and are loaded to bigquery directly in the specified dataset.
# 

from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder.appName("LoadFilesToDataFrames").getOrCreate()

# Folder path where files are located
bucket = r"gs://test-gsynergy/"
file =[
    'fact.averagecosts.dlm.gz',
'fact.transactions.dlm.gz',
'hier.clnd.dlm.gz',
'hier.hldy.dlm.gz',
'hier.invloc.dlm.gz',
'hier.invstatus.dlm.gz',
'hier.possite.dlm.gz',
'hier.pricestate.dlm.gz',
'hier.prod.dlm.gz',
'hier.rtlloc.dlm.gz']

project_id = 'gsynergy-452808'
dataset_id = 'gsynergy'
for i in file:
    file_path = bucket+i
    table_id = i[0:4]+'-'+i[5:-7] #this is for creating the tablename from the files and remove the extention from table name
    print(file_path)
    df = spark.read.option("delimiter", "|").csv(file_path, header=True, inferSchema=True)
    df.printSchema()
    df.show(3)

    bigquery_table = f"{project_id}:{dataset_id}.{table_id}"

    df.write.format('bigquery') \
        .option('table', bigquery_table) \
        .option('temporaryGcsBucket', 'test-gsynergy') \
        .save()
    print('completed writing table: '+ table_id)
spark.stop()

