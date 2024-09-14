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

ddl_path = f"{volume}/{ddl_path}"
target_folder = f"{volume}/{target_folder}"

# string to bool transform
folders = ['view']

for folder in folders:
  exec(f"{folder}_path='{ddl_path}/{folder}'")
  exec(f"params['{folder}_path']='{ddl_path}/{folder}'")

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,views
# create objects
fldr = _create_ddl_folder(target_folder,'view')

# rearrange list
files = [x.path for x in dbutils.fs.ls(view_path) if 'drop_view' not in x.path]
enc = [x.path for x in dbutils.fs.ls(view_path) if '/encounter_view' in x.path][0]
files.remove(enc)
files.append(enc)

# views
for file in files:

  view = file.split('/')[-1].split('.')[0]

  print(f"View: \t{Fore.YELLOW+view+Fore.WHITE}")

  df = spark.read.text(file,lineSep=";")

  df_view = (df
            .withColumn('value', F.replace(F.col("value"),F.lit(f'{source_schema}.'),F.lit(f'{db_catalog}.{db_schema}.')))
            .withColumn('value', F.replace(F.col("value"),F.lit('public.'),F.lit(f'{db_catalog}.{db_schema}.')))
            .withColumn('value', F.replace(F.col("value"),F.lit('cascade'),F.lit(""))))

  queries = [x[0] for x in df_view.collect()]
  
  _write_ddl(fldr,view,'',queries)
  
  print(f'\tview {Fore.YELLOW+view+Fore.WHITE} created...\n' )

print(Fore.GREEN+'\033[1m'+'Done createing views!')
