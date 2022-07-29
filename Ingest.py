# Databricks notebook source
# MAGIC %pip install azure-eventhub

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from azure.eventhub import EventData, EventHubProducerClient, EventHubSharedKeyCredential
import datetime
import requests
import os
import json


# COMMAND ----------

# Twitter Auth

bearer_token = dbutils.secrets.get(scope = "TwitterStreamKV", key = "BearerToken")
auth = { "Authorization" : f"Bearer {bearer_token}", "User-Agent" : "v2FilteredStreamPython"}

# COMMAND ----------

# Filtered Stream access

rules = [ { "value" : "(#UkraineWare OR #WARINUKRAINE) -is:retweet lang:en", "tag" : "#UkraineWar tag" } ]
payload = { "add" : rules }
response = requests.post("https://api.twitter.com/2/tweets/search/stream/rules",
                         headers = auth, json = payload)

# COMMAND ----------

# Connecting to Event Hub

eventhub_policy = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubPolicy")
eventhub_name = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubName")
eventhub_namespace = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubNamespace")
eventhub_sas = dbutils.secrets.get(scope="TwitterStreamKV", key = "EventHubSAS")
 
shared_key_credential = EventHubSharedKeyCredential(eventhub_policy,eventhub_sas)
eh_producer_client = EventHubProducerClient(fully_qualified_namespace=eventhub_namespace, credential=shared_key_credential, eventhub_name=eventhub_name)


# COMMAND ----------

response = requests.get(
    "https://api.twitter.com/2/tweets/search/stream?expansions=author_id&tweet.fields=created_at,public_metrics", headers = auth, stream=True,
)
 
if response.status_code != 200:
    raise Exception(
        "Cannot get stream (HTTP {}): {}".format(
            response.status_code, response.text
        )
    )
increment =  0
for response_line in response.iter_lines():
    if response_line:
        increment += 1
        print(f"Message {increment} received")
        event_data_batch = ehpc.create_batch()
        data = EventData(body=response_line)
        event_data_batch.add(data)
        ehpc.send_batch(event_data_batch)

# COMMAND ----------

