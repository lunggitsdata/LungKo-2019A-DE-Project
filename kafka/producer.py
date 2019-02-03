#!/usr/bin/python

# This code reads data from an S3 bucket and
# passes it to a Kafka topic via a KafkaProducer
# Kept essentially identical to
# https://github.com/rkhebel/Insight-DE-2018C-Project/blob/master/kafka/producer.py
# for ease of comparison

from kafka import KafkaProducer

import boto3
import botocore
import pandas as pd

# Producer running on one (and only one) of the Kafka nodes
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

s3 = boto3.resource('s3', aws_access_key_id = '', aws_secret_access_key = '')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')

# Loop through objects. Each object.key is a pointer to a csv file
for object in bucket.objects.all():
    # skip non-trading hours by file size
    if object.size > 136:
        url = 'https://s3.eu-central-1.amazonaws.com/deutsche-boerse-xetra-pds/' + object.key
        data = pd.read_csv(url)
        #read through each line of csv and send the line to the kafka topic
        for index, row in data.iterrows():
            output = ''
            for element in row:
                output = output + str(element) + "^"
            
            producer.send('stocksinput', output.encode())
            producer.flush()
