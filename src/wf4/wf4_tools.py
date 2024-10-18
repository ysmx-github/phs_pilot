# Databricks notebook source
# MAGIC %md
# MAGIC ### Function module
# MAGIC -  Imports
# MAGIC -  Utilities, table and DDL creation functions, status table functions and validation checks
# MAGIC -  Merge statement genarator
# MAGIC

# COMMAND ----------

# DBTITLE 1,imports
import yaml, datetime, random
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,utilities
## read YAML file #########################################################################
_get_config = lambda yaml_file: yaml.full_load(open(yaml_file))

### fully qualified table name ############################################################
_get_table_fq = lambda catalog, target_schema, target_table: f'{catalog}.{target_schema}.{target_table}'

# COMMAND ----------

# DBTITLE 1,table and ddl file creation
## create DDL scripts output folder #######################################################
def _create_ddl_folder (catalog,schema,volume,target_folder):
  fldr = f"/Volumes/{catalog}/{schema}/{volume}/{target_folder}"
  dbutils.fs.mkdirs(fldr)
  return fldr

## cast double to ling #######################################################
def _cast_df(df):
  for x in df.schema.jsonValue()['fields']:
    if x['type'] == 'double':
      df = df.withColumn(x['name'], F.col(x['name']).cast('long'))
  return df

## create target table ####################################################################
def _create_table (template_file, target_table_fq=None, catalog=None,target_schema=None,target_table=None):

  if not target_table_fq: target_table_fq = _get_table_fq (catalog,target_schema,target_table)

  spark.sql(f'drop table if exists {target_table_fq}')

  df = (spark
        .read
        .parquet(template_file)
        .limit(100)
        .drop('newline'))

  df = _cast_df(df)

  df.write.saveAsTable(target_table_fq)

  spark.sql(f"truncate table {target_table_fq}")

## alter table add not nullable fields ####################################################
def _not_null_fields (table_params, target_table_fq, create = False ):
  sqls = []
  not_null_fields = table_params['not_null_fields']
  if not_null_fields:
    for field in not_null_fields:
      sql = f"alter table {target_table_fq} alter column {field} set not null"
      if create: spark.sql(sql)
      sqls.append(sql)
  return sqls

## alter table add primary key ############################################################
def _primary_key (table_params, target_table_fq, create = False ):
  primary_key = table_params['primary_key']
  if primary_key:
    sql = f"alter table {target_table_fq} add constraint {primary_key} primary key ({primary_key})"
    if create: spark.sql(sql)
  return sql

## alter table add foreign keys ###########################################################
def  _foreign_keys (table_params, target_table_fq, catalog, target_schema, create = False):
  sqls = []
  foreign_keys = table_params['foreign_keys']
  if foreign_keys:
    for fk in foreign_keys:
      reference_table = fk['reference_table']
      reference_key = fk['reference_key']
      reference_table_fq = _get_table_fq (catalog,target_schema,reference_table)

      sql = f"""
alter table {target_table_fq} 
  add constraint {target_table}_{reference_table}_{fk['name']} 
  foreign key ({reference_key}) 
  references {reference_table_fq} ({reference_key});
"""
      if create: spark.sql(sql)
      sqls.append(sql)

  return sqls

## write DDL file #########################################################################
def _write_ddl (catalog,schema,volume,target_folder,target_table,table_params):
  
  target_table_fq = _get_table_fq (catalog,target_schema,target_table)

  df = spark.table(target_table_fq)

  cr_sql = f"""create or replace table {target_table_fq} \n\t( """ \
          +'\n\t, '.join([' '.join([fld['name'],fld['type']]) for fld in df.schema.jsonValue()['fields']]) \
          +'\n\t);\n'

  _create_ddl_folder(catalog,schema,volume,target_folder)

  with open(f"/Volumes/{catalog}/{schema}/{volume}/{target_folder}/{target_table}.create.sql", "w") as fw:
    
    # create sql
    fw.write(cr_sql)

    # not nulls
    for sql in _not_null_fields (table_params, target_table_fq): 
      fw.write(f'\n{sql};')

    # primary key
    fw.write(f'\n\n{_primary_key (table_params, target_table_fq)};') 

    # foreign keys
    for sql in _foreign_keys (table_params, target_table_fq, catalog, schema): 
      fw.write(f'\n{sql}')

  return cr_sql

# COMMAND ----------

# DBTITLE 1,control
## insert record into control table #########################################################
def _insert_control_table (catalog, target_schema, control_table, file_name, cnt):
  spark.sql(f"""insert into {catalog}.{target_schema}.{control_table}  
                  replace where file_name='{file_name}'
                  values ('{file_name}',current_timestamp(),{cnt},Null,false)                                          
            """)

## update control table record ##############################################################
def _update_control_table (catalog, target_schema, control_table, file):
  spark.sql(f"""update {catalog}.{target_schema}.{control_table}  
                  set file_loaded = true 
                      ,file_processed_timestamp = current_timestamp
                where file_name = '{file.file_name}'
              """)

# COMMAND ----------

# DBTITLE 1,check
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
  return match

## symmetric fileds match check #############################################################
def _match_field_names_symmetric(df, catalog, target_schema, target_table, drop = ['delta_flg']):
  dff = set(df.drop(*drop).schema.names)

  tbf = set([x.column_name for x in 
             spark.sql(f"""select column_name
                             from {catalog}.information_schema.columns 
                            where table_catalog = '{catalog}' 
                              and table_schema = '{target_schema}'
                              and table_name = '{target_table}'""").collect()])
  return not bool(dff^tbf)

# COMMAND ----------

# DBTITLE 1,merge
## file merge #########################################################################
def generate_merge_sql(catalog, target_schema, source_schema, volume, stage_folder, file, load_type):  

  file_name = file['file_name']
  file_path = f"/Volumes/{catalog}/{source_schema}/{volume}/{stage_folder}/{file_name}"
  target_table = file['tbl']

  for x in target_tables:
    if target_table in x.keys():
      table_params = x[target_table]  
  
  target_table_fq = _get_table_fq (catalog,target_schema,target_table)

  primary_key = table_params['primary_key']

  df = spark.read.parquet(file_path)

  df.createOrReplaceTempView("temp_view")

  filed_names = [x for x in df.schema.names if x != 'delta_flg']

  upd = '\n\t,'.join([f"target.{x} = source.{x}" for x in filed_names])
  fld = '\n\t,'.join(filed_names)
  val = '\n\t,'.join([f"source.{x}" for x in filed_names])

  sql = f"""
  merge into {target_table_fq} as target
  using temp_view as source
  on target.{primary_key} = source.{primary_key}
    when matched 
     and source.delta_flg = 'U' 
    then
      update set 
      \t {upd}
    when matched 
     and source.delta_flg = 'D' 
    then
      delete  
    when not matched 
      {"and source.delta_flg = 'I'" if load_type == 'Incremental' else ''}
    then
      insert (
      \t {fld}
    ) values (
      \t {val}
    );
  """
  
  return sql