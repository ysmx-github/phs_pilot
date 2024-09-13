# Databricks notebook source
import dlt

@dlt.view(name="patient")
def source():
 return spark.read.table("ysm.clinical.patient")

dlt.create_streaming_table("patient_dlt")

dlt.apply_changes_from_snapshot(target="patient_dlt"
                                ,source="patient"
                                ,keys=["patient_sk"]
                                ,stored_as_scd_type=1)
