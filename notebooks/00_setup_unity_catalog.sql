-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_silver;
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_gold;
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_ops;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.ecommerce_bronze.raw_files;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.ecommerce_ops.pipeline_audit_log (
    run_id STRING,
    batch_id STRING,
    layer STRING,
    table_name STRING,
    status STRING,
    total_rows BIGINT,
    corrupt_rows BIGINT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_sec DOUBLE,
    error_message STRING
) USING DELTA;
