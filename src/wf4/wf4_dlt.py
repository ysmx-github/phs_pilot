# Databricks notebook source
# DBTITLE 1,checks
## ordinality check #########################################################################
def _match_ordinality(df, catalog, target_schema, target_table):

  tbl = spark.sql(f"""select * 
                        from {catalog}.information_schema.columns 
                      where table_catalog = '{catalog}' 
                        and table_schema = '{target_schema}'
                        and table_name = '{target_table}'""").collect()

  match = True
  for f in tbl:
    if f.ordinal_position != df.columns.index(f.column_name):
      match = False 
  return str(match)

## symmetric fileds match check #############################################################
def _match_field_names_symmetric(df, catalog, target_schema, target_table, drop = ['delta_flg']):
  dff = set(df.drop(*drop).schema.names)

  tbf = set([x.column_name for x in 
             spark.sql(f"""select column_name
                             from {catalog}.information_schema.columns 
                            where table_catalog = '{catalog}' 
                              and table_schema = '{target_schema}'
                              and table_name = '{target_table}'""").collect()])
  ret = not bool(dff^tbf)
  return str(ret)

# COMMAND ----------

# DBTITLE 1,params
catalog = 'ysm'
target_schema = 'premiere'
files = f"/Volumes/ysm/ysm_schema/ysm_schema_vol/premiere/stage/patient_ins.*.parquet*/*parquet"
source = 'patient_ins_mv'
target = "patient_ins_dlt"

# COMMAND ----------

# DBTITLE 1,dlt
import dlt

import dlt
import pyspark.sql.functions as F

df = spark.read.parquet(files)

@dlt.table (name = source)
@dlt.expect_or_fail (_match_ordinality(df, catalog, target_schema, source), 'True')
@dlt.expect_or_fail (_match_field_names_symmetric (df, catalog, target_schema, source), 'True')
def patient_ins_mv():
  return spark.readStream.schema(df.schema).parquet(files)

dlt.create_target_table (target)

dlt.apply_changes (
  target = target,
  source = source,
  keys = ["patient_sk"],
  sequence_by = F.to_unix_timestamp(F.col("record_update_timestamp")),
  apply_as_deletes = "delta_flg = 'D'",
  except_column_list = ["delta_flg"],
  stored_as_scd_type = 1
)
