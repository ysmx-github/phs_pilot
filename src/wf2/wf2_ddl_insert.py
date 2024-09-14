# Databricks notebook source
# DBTITLE 1,imports
# MAGIC %run ./../wf_common/f_common

# COMMAND ----------

# DBTITLE 1,remove wirgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,parameters
# widget
dbutils.widgets.text('yaml_file','./../wf_common/config.yaml',"CONFIG FILE")

# config yaml file
yaml_file = dbutils.widgets.get('yaml_file')

conf = _get_config(yaml_file)

# params dict
params = { 'yaml_file': yaml_file
          ,"ddl_path": conf['clinical']['ddl_path']
          ,"folders": conf['clinical']['folders']
          ,"source_schema": conf['clinical']['source_schema']
          ,"db_catalog": conf['dbr']['db_catalog']          
          ,"db_schema": conf['dbr']['db_schema']
          ,"mode": conf['dbr']['mode']
          ,'volume': conf['dbr']['volume']
          ,"target_folder": conf['dbr']['target_folder']}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

volume = f"/Volumes/{db_catalog}/{volume}"
ddl_path = f"{volume}/{ddl_path}"
target_folder = f"{volume}/{target_folder}"

# string to bool transform
folders = ['data']

for folder in folders:
  exec(f"{folder}_path='{ddl_path}/{folder}'")
  exec(f"params['{folder}_path']='{ddl_path}/{folder}'")

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,inserts
for file in [x.path for x in dbutils.fs.ls(data_path)]:

  table = file.split('/')[-1].split('.')[0].replace('insert_','').replace('_default','')

  print(f"Table: \t{Fore.YELLOW+table+Fore.WHITE}")

  df = spark.read.text(file,lineSep=";")

  df_insert = (df
  .withColumn('value', F.replace(F.col("value"),F.lit("YYYY"), F.lit("yyyy")))
  .withColumn('value', F.replace(F.col("value"),F.lit("DD"),F.lit("dd")))
  .withColumn('value', F.replace(F.col("value"),F.lit("MI"), F.lit("mm")))
  .withColumn('value', F.replace(F.col("value"),F.lit("SS"),F.lit("ss")))
  .withColumn('value', F.replace(F.col("value"),F.lit("HH24"), F.lit("kk")))
  .withColumn('value', F.replace(F.col("value"),F.lit("HH12"), F.lit("hh")))
  .withColumn('value', F.replace(F.col("value"),F.lit(f'{source_schema}.'),F.lit(f'{db_catalog}.{db_schema}.'))))

  sql_truncate = f"truncate table {db_catalog}.{db_schema}.{table}"
  # print(sql_truncate)
  spark.sql(sql_truncate)
  print(f'\ttable {Fore.YELLOW+table+Fore.WHITE} truncated...' )

  sql_insert = df_insert.first()[0]
  # print(sql_insert)
  spark.sql(sql_insert)
  print(f'\ttable {Fore.YELLOW+table+Fore.WHITE} loaded...\n' )
  
  # write databricks ddl to file
  fldr = _create_ddl_folder(target_folder, 'data')
  _write_ddl(fldr,f'insert_{table}_default',sql_insert)
  
print(Fore.GREEN+'\033[1m'+'Done inserting data!')
