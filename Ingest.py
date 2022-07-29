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
auth = { "Authotization" : f"Bearer {bearer_token}", "User-Agent" : "v2FilteredStreamPython"}

# COMMAND ----------

# Filtered Stream access
rules = [ { "value" : "(#UkraineWare OR #WARINUKRAINE) -is:retweet lang:en", "tag" : "#UkraineWar tag" } ]
payload = { "add" : rules }
response = requests.post("https://api.twitter.com/2/tweets/search/stream/rules",
                         headers = auth, json = payload)

# COMMAND ----------

