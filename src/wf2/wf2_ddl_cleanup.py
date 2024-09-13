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
          ,"db_catalog": conf['dbr']['db_catalog']          
          ,"db_schema": conf['dbr']['db_schema']
          ,"target_folder": conf['dbr']['target_folder']}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# string to bool transform
folders = eval(folders)

for folder in folders:
  exec(f"{folder}_path='{ddl_path}/{folder}'")
  exec(f"params['{folder}_path']='{ddl_path}/{folder}'")

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,cleanup
# cleanup
_cleanup(db_catalog,db_schema)
  
# files
try:
  for f in [x.path for x in dbutils.fs.ls(f'{target_folder}/table')] \
          +[x.path for x in dbutils.fs.ls(f'{target_folder}/view')] \
          +[x.path for x in dbutils.fs.ls(f'{target_folder}/data')] \
          +[x.path for x in dbutils.fs.ls(f'{target_folder}/index')]:
    dbutils.fs.rm(f)
except:
  pass

print(Fore.YELLOW+'\033[1m'+'Cleanup done!')