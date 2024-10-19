# Databricks notebook source
# MAGIC %md
# MAGIC ### File generator 
# MAGIC - Generates initial load and incremental load stage files
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
# widgets
dbutils.widgets.text('yaml_file','wf4_config.yaml',"01. CONFIG FILE")
dbutils.widgets.dropdown("file_type","Initial", [ "I","U","D","Initial"],"03. FILE TYPE")

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
          ,"control_table": conf['parquet']['control_table']
          ,'file_type': dbutils.widgets.get("file_type")}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,initial load
raw_file_location = f'/Volumes/{catalog}/{source_schema}/{volume}/{source_folder}'

stage_location = f'/Volumes/{catalog}/{source_schema}/{volume}/{stage_folder}'

# list of files
file_names = [x.name for x in dbutils.fs.ls(raw_file_location)]

if file_type == 'Initial':
  for file_name in file_names:
    
    # transform and copy all files
    df = spark.read.parquet(f'{raw_file_location}/{file_name}').withColumnRenamed('newline','delta_flg')
    df = _cast_df(df)
    df.write.mode('overwrite').parquet(f'{stage_location}/{file_name}')


    cnt = df.count()

    delta_flags = ', '.join([x[0] for x in df.select('delta_flg').distinct().collect()])
    
    # insert row into the status table
    _insert_control_table (catalog, target_schema, control_table, file_name, cnt)

    # print result
    print(f"Copied file {raw_file_location}/{file_name} ({cnt} rows, delta flags: {delta_flags})\n\tto {stage_location}/{file_name}")
else:
  print('Did not copy any initial load files')

# COMMAND ----------

# DBTITLE 1,incremental load
random_file_name = file_names[random.randrange(0,len(file_names))]
tiles = 5
random_tile = random.randrange(1, tiles+1)

target_table = random_file_name.split('.')[0]

if file_type == 'Initial':
  print('Did not generate any incremental load files')
else:
  if file_type == 'D':
    df = (spark
          .read
          .parquet(f'{raw_file_location}/{random_file_name}')
          .drop('newline')
          .withColumn ('delta_flg',F.lit(file_type))
          .withColumn ('t',F.ntile(tiles).over(Window.orderBy(F.lit(None))))
          .where(F.col('t') == F.lit(random_tile))
          .drop('t')
          .withColumn('record_update_timestamp', F.from_unixtime(F.unix_timestamp('record_update_timestamp')+10).cast('timestamp')))
    
  elif file_type == 'U':
    df = (spark
          .read
          .parquet(f'{raw_file_location}/{random_file_name}')
          .drop('newline')
          .withColumn ('delta_flg',F.lit(file_type))
          .withColumn ('t',F.ntile(tiles).over(Window.orderBy(F.lit(None))))
          .where(F.col('t') == F.lit(random_tile))
          .drop('t')
          .withColumn('record_insert_id', F.lit('databricks'))
          .withColumn('record_update_timestamp', F.from_unixtime(F.unix_timestamp('record_update_timestamp')+10).cast('timestamp')))
    
  elif file_type == 'I':
    max_id = spark.table(f"{catalog}.{target_schema}.{target_table}").selectExpr("max(patient_sk) max_id").first()[0]
    df = (spark
          .read
          .parquet(f'{raw_file_location}/{random_file_name}')
          .drop('newline')
          .withColumn ('delta_flg',F.lit(file_type))
          .withColumn ('t',F.ntile(tiles).over(Window.orderBy(F.lit(None))))
          .where(F.col('t') == F.lit(random_tile))
          .withColumn('rn', F.row_number().over(Window.orderBy(F.lit(None))))
          .withColumn('patient_sk', F.col('rn') + max_id)
          .drop('t','rn'))
  
  cnt = df.count()

  now = int(datetime.datetime.now().timestamp())

  fn = f'{random_file_name}_{file_type}_{now}'
  fnfq = f'{stage_location}/{random_file_name}_{file_type}_{now}'

  # checks and load
  if _match_ordinality (df, catalog, target_schema, target_table) and _match_field_names_symmetric(df, catalog, target_schema, target_table):

    # write the file
    df = _cast_df(df)
    df.write.mode('overwrite').parquet(fnfq)
    
    # insert row into the status table
    _insert_control_table (catalog, target_schema, control_table, fn, cnt)

    # print result
    print(f"Created file {fn} ({cnt} rows, delta flags: {file_type})")

    df.display()
  else:
    print ("Checks failed")



# COMMAND ----------

# DBTITLE 1,validation
# MAGIC %sql
# MAGIC select * from ysm.premiere.batch_file_control order by file_arrival_timestamp;