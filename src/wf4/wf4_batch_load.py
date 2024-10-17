# Databricks notebook source
# MAGIC %md
# MAGIC ### Loader 
# MAGIC - Loads initial load and incremental load files
# MAGIC - Catalog, source, stage and target schema, volume and locations are set in the configuration YAML file
# MAGIC - Type of the load controled by the dropdown widget

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
dbutils.widgets.text('yaml_file','wf4_config.yaml',"01. CONFIG FILE")
dbutils.widgets.dropdown("load_type","Initial", [ "Initial","Incremental"],"02. LOAD TYPE")

yaml_file = dbutils.widgets.get('yaml_file')

conf = _get_config(yaml_file)

# params dict
params = { 'yaml_file': yaml_file
          ,"catalog": conf['parquet']['catalog']
          ,"source_schema": conf['parquet']['source_schema']
          ,"target_schema": conf['parquet']['target_schema']
          ,"volume": conf['parquet']['volume']
          ,"source_folder": conf['parquet']['source_folder']
          ,"stage_folder": conf['parquet']['stage_folder']
          ,"target_tables": conf['parquet']['target_tables']
          ,"control_table": conf['parquet']['control_table']
          ,"load_type": dbutils.widgets.get('load_type')}


# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# hard-coded
target_tables = eval(str(target_tables))

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,load
# list new arrived files
files = spark.sql(f"""select file_name
                             ,split_part(file_name, '.', 1) tbl
                         from {catalog}.{target_schema}.batch_file_control 
                        where not file_loaded
                    """).collect()

if load_type == 'Initial':
  files = [x for x in files if 'parquet_' not in x.file_name]

# load files in the list
for file in files:

  merge_sql = generate_merge_sql(catalog, target_schema, source_schema, volume, stage_folder, file, load_type)
  
  # print(merge_sql)

  spark.sql(merge_sql)

  # update status table
  _update_control_table (catalog, target_schema, control_table, file)

  # print result
  print (f"""Loaded file /Volumes/{catalog}/{source_schema}/{volume}/{stage_folder}/{file['file_name']} 
             into table {catalog}.{target_schema}.{file['tbl']}""")

# COMMAND ----------

# DBTITLE 1,validation
# MAGIC %sql
# MAGIC select vendor_code, client_code, count(*) from ysm.premiere.patient_ins group by 1,2;