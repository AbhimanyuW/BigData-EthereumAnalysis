import sys
import os
import time
import operator
import boto3
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import StringType

if __name__ == "__main__":
    
    # create SparkSession
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()


    # read configuration from environment variables
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # configure Hadoop with S3 credentials
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # read transactions from S3 and filter out invalid transactions
    transactions_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "DROPMALFORMED") \
        .load("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")


    # convert timestamp to month/year format and group by month/year
    total_transactions = transactions_df.withColumn(
        "month_year",
        from_unixtime(transactions_df["block_timestamp"].cast("bigint"), "MM/yyyy")) \
        .groupBy("month_year") \
        .count() \
        .orderBy("month_year")

    # write result to S3
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    #current date,time ->
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    result_str = total_transactions.limit(100).toJSON().collect()
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'new_part_A' + date_time + '/total_txn.txt')
    my_result_object.put(Body=json.dumps(result_str))

    # stop SparkSession
    spark.stop()