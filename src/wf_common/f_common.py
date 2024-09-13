# Databricks notebook source
# DBTITLE 1,imports
import yaml
import pyspark.sql.functions as F
from colorama import Fore

# COMMAND ----------

# DBTITLE 1,functions
_get_config = lambda yaml_file: yaml.full_load(open(yaml_file))

def _create_ddl_folder(target_folder, obj):
  fldr = f"{target_folder}/{obj}/"
  dbutils.fs.mkdirs(fldr)
  return fldr

def _write_ddl(fldr,object,sql='',l=[]):
  with open(f"{fldr}/{object}.sql", "w") as fw:
    if l:
      for e in l:
        fw.write(e)
        fw.write(';')
    else:
      fw.write(sql)
      fw.write(';')

def _cleanup(db_catalog,db_schema,obj = 'all'):
  cleanup_df = spark.sql(f"""select table_name, table_type 
                               from {db_catalog}.information_schema.tables 
                              where table_catalog = '{db_catalog}' 
                                and table_schema = '{db_schema}'
                             """)
  
  if obj in ['table','all']:

    # tables
    df = cleanup_df.where("table_type <> 'VIEW'").drop('table_type')
    objects = [x.table_name for x in df.collect()]

    for object in objects:
      spark.sql(f"drop table if exists {db_catalog}.{db_schema}.{object}")

  if obj in ['view','all']:

    # views
    df = cleanup_df.where("table_type = 'VIEW'").drop('table_type')  
    objects = [x.table_name for x in df.collect()]

    for object in objects:
      spark.sql(f"drop view if exists {db_catalog}.{db_schema}.{object}")