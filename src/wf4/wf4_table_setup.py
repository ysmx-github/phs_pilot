# Databricks notebook source
# MAGIC %md
# MAGIC ### Tables and DDL files generator
# MAGIC - Full refresh: rebuilds all tables and overwrites all DDL scripts
# MAGIC - Generates empty tables, doesn't load any data
# MAGIC - Catalog, source and target schema, volume and locations are set in the configuration YAML file

# COMMAND ----------

# DBTITLE 1,installations
# MAGIC %pip install pyyaml
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,imports
# MAGIC %run ./wf4_tools

# COMMAND ----------

# DBTITLE 1,remove widgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,parameters
# widget
dbutils.widgets.text('yaml_file','wf4_config.yaml',"CONFIG FILE")

yaml_file = dbutils.widgets.get('yaml_file')

conf = _get_config(yaml_file)

# params dict
params = { 'yaml_file': yaml_file
          ,"catalog": conf['parquet']['catalog']
          ,"source_schema": conf['parquet']['source_schema']
          ,"target_schema": conf['parquet']['target_schema']
          ,"volume": conf['parquet']['volume']
          ,"source_folder": conf['parquet']['source_folder']
          ,"target_folder": conf['parquet']['target_folder']
          ,"target_tables": conf['parquet']['target_tables']}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# hard-coded
target_tables = eval(str(target_tables))

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,create initial tables
for d in target_tables:  

  target_table = list(d.keys())[0]
  
  table_params = list(d.values())[0]
  
  template_file = table_params['template_file']

  target_table_fq = _get_table_fq (catalog,target_schema,target_table)
  
  # cretae table
  _create_table (template_file, target_table_fq)

  # add not null constraints
  _not_null_fields (table_params, target_table_fq, True )

  # add primary key
  _primary_key (table_params, target_table_fq, True )
  
  # add foreign keys
  _foreign_keys (table_params, target_table_fq, catalog, target_schema, True )

  print(f"Created table {target_table_fq}")

# COMMAND ----------

# DBTITLE 1,generate ddl files
for d in target_tables:
  
  # generate DDL script
  target_table = list(d.keys())[0]
  _write_ddl ( catalog
              ,source_schema  
              ,volume
              ,target_folder
              ,target_table
              ,table_params)
  
  # read back and print DDL script
  with open(f"/Volumes/{catalog}/{source_schema}/{volume}/{target_folder}/{target_table}.create.sql", "r") as fw:
    print(fw.read())

# COMMAND ----------

# DBTITLE 1,cleanup
spark.sql(f"alter table {catalog}.{target_schema}.{target_table} drop constraint if exists patient_ins_practitioner_fk1");
dbutils.fs.rm(f"/Volumes/{catalog}/{source_schema}/{volume}/premiere/stage/", True)

# COMMAND ----------

# DBTITLE 1,validation
spark.sql(f"select * from {catalog}.information_schema.columns where table_name = '{target_table}'").display()
spark.sql(f"select * from {catalog}.information_schema.table_constraints where table_name = '{target_table}'").display()

# COMMAND ----------

# DBTITLE 1,batch_file_control
# MAGIC %sql
# MAGIC create or replace table ysm.premiere.batch_file_control 
# MAGIC    ( file_name string
# MAGIC    , file_arrival_timestamp timestamp
# MAGIC    , file_number_of_rows bigint
# MAGIC    , file_processed_timestamp timestamp
# MAGIC    , file_loaded boolean)
# MAGIC ;
# MAGIC
# MAGIC describe extended ysm.premiere.batch_file_control; 