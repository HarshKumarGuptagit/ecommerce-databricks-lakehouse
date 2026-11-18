# Databricks notebook source
import sys, os, uuid
sys.path.append(os.path.abspath(".."))

from src.config import DATASETS
from src.ingestion.bronze_loader import ingest_to_bronze

# COMMAND ----------

dbutils.widgets.text("batch_id", "initial_full_load")
batch_id = dbutils.widgets.get("batch_id")
run_id = str(uuid.uuid4()) 

# COMMAND ----------

for name, fname in DATASETS.items():
    result = ingest_to_bronze(spark, name, fname, batch_id=batch_id, run_id=run_id)
    print(result)
 