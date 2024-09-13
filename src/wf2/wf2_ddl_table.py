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
          ,"target_folder": conf['dbr']['target_folder']}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# string to bool transform
folders = ['table']

for folder in folders:
  exec(f"{folder}_path='{ddl_path}/{folder}'")
  exec(f"params['{folder}_path']='{ddl_path}/{folder}'")

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,tables

# create objects
spark.sql(f'create schema if not exists {db_catalog}.{db_schema}')
fldr = _create_ddl_folder(target_folder,'table')

# create tables
for file in [x.path for x in dbutils.fs.ls(table_path)]:

  table = file.split('/')[-1].split('.')[0]

  print(f"Table: \t{Fore.YELLOW+table+Fore.WHITE}")

  df = spark.read.text(file,lineSep=";")
  df_create = df.filter(F.col("value").ilike("%create table%"))

  # prepare create statement
  df_create = (df_create
  .withColumn('value', F.replace(F.col("value"),F.lit("int8"), F.lit("bigint")))
  .withColumn('value', F.replace(F.col("value"),F.lit("int4"), F.lit("int")))
  .withColumn('value', F.replace(F.col("value"),F.lit("text"), F.lit("string")))
  .withColumn('value', F.replace(F.col("value"),F.lit("NULL"),F.lit("")))
  .withColumn('value', F.replace(F.col("value"),F.lit("NOT"),F.lit("NOT NULL")))
  .withColumn('value', F.replace(F.col("value"),F.lit(f'{source_schema}.'),F.lit(f'{db_catalog}.{db_schema}.'))))

  sql_create = df_create.first()[0]
  # print(sql_create)

  # prepare drop statement, drop table if needed
  sql_drop = sql_create.split('(')[0].lower().replace("create table","drop table if exists")
  # print(sql_drop)
  spark.sql(sql_drop)
  print(f'\ttable {Fore.YELLOW+table+Fore.WHITE} dropped...' )
  
  # create table
  spark.sql(sql_create)
  print(f'\ttable {Fore.YELLOW+table+Fore.WHITE} created...\n' )

  # write databricks ddl to file
  _write_ddl(fldr,table,sql_create)

print(Fore.GREEN+'\033[1m'+'Done creating tables!')
