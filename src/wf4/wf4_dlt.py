# Databricks notebook source
# DBTITLE 1,functions
## ordinality check #########################################################################
def _match_ordinality(df, catalog, target_schema, target_table, drop = ['_rescued_data']):

  drop_str = ','.join(drop)

  tbl = spark.sql(f"""select * 
                        from {catalog}.information_schema.columns 
                      where table_catalog = '{catalog}' 
                        and table_schema = '{target_schema}'
                        and table_name = '{target_table}'
                        and column_name not in ('{drop_str}')
                        """).collect()

  match = True
  for f in tbl:
    if f.ordinal_position != df.columns.index(f.column_name):
      match = False 
  return str(match)

## symmetric fileds match check #############################################################
def _match_field_names_symmetric(df, catalog, target_schema, target_table, drop = ['delta_flg','_rescued_data']):

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
# from pipeline config
catalog = spark.conf.get('catalog')
target_schema = spark.conf.get('target_schema')
source_schema = spark.conf.get('source_schema')
volume = spark.conf.get('volume')
stage_folder = spark.conf.get('stage_folder')

# hard-coded
files = f"/Volumes/{catalog}/{source_schema}/{volume}/{stage_folder}/patient_ins.*.parquet*/*parquet"

source_st = 'patient_ins_st'
source_vv = 'patient_ins_vv'
target = "patient_ins_dlt"

# COMMAND ----------

# DBTITLE 1,dlt
import dlt
import pyspark.sql.functions as F

# read files using Autoloader
@dlt.table (name = source_st)
def ft():
  return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(files)

if not spark.table(f"{catalog}.information_schema.tables").where("table_schema = '{target_schema}' and table_name = '{source_st}'").collect():

  # initial load - table create, requires full refresh
  @dlt.view (name = source_vv)
  def fv():
    return dlt.read_stream(source_st)

else:
  
  # load to existing table, incremental
  df = spark.table(f"{catalog}.{target_schema}.{source_st}")
  @dlt.view (name = source_vv)
  @dlt.expect_all_or_fail ({ _match_ordinality(df, catalog, target_schema, target):'True'
                           , _match_field_names_symmetric (df, catalog, target_schema, target):'True'})
  def fv():
    return dlt.read_stream(source_st)

# create or refresh target table
dlt.create_streaming_table (target)

# incremental CDC
dlt.apply_changes (
  target = target,
  source = source_vv,
  keys = ["patient_sk"],
  sequence_by = "record_update_timestamp",
  apply_as_deletes = "delta_flg = 'D'",
  except_column_list = ["delta_flg"],
  stored_as_scd_type = 1
)
