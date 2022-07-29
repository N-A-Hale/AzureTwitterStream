# Databricks notebook source
# MAGIC %pip install azure-eventhub

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import datetime
import requests
import os
import json