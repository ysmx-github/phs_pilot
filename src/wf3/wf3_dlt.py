# Databricks notebook source
import dlt

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

source_table = 'patient'

@dlt.view(name=source_table)
def source():
 return spark.read.table(f"{catalog}.{schema}.{source_table}")

dlt.create_streaming_table(f"{source_table}_dlt")

dlt.apply_changes_from_snapshot(target=f"{source_table}_dlt"
                                ,source=source_table
                                ,keys=[f"{source_table}_sk"]
                                ,stored_as_scd_type=1)
