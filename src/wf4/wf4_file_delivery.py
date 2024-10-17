# Databricks notebook source
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
          ,"stage_folder": conf['parquet']['stage_folder']}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,full files
raw_file_location = f'/Volumes/{catalog}/{source_schema}/{volume}/{source_folder}'

stage_location = f'/Volumes/{catalog}/{source_schema}/{volume}/{stage_folder}'

# list of files
file_names = [x.name for x in dbutils.fs.ls(raw_file_location)]

for file_name in file_names:
  
  # transform and copy all files
  df = spark.read.parquet(f'{raw_file_location}/{file_name}').withColumnRenamed('newline','delta_flg')
  
  df.write.mode('overwrite').parquet(f'{stage_location}/{file_name}')

  cnt = df.count()

  delta_flags = ', '.join([x[0] for x in df.select('delta_flg').distinct().collect()])
  
  # insert row into the status table
  spark.sql(f"""insert into {catalog}.{target_schema}.batch_file_control  
                  replace where file_name='{file_name}'
                  values ('{file_name}',current_timestamp(),{cnt},Null,false)                                          
            """)

  # print result
  print(f"Copied file {raw_file_location}/{file_name} ({cnt} rows, delta flags: {delta_flags})\n\tto {stage_location}/{file_name}")

# COMMAND ----------

# DBTITLE 1,validation
# MAGIC %sql
# MAGIC select * from ysm.premiere.batch_file_control order by file_arrival_timestamp;