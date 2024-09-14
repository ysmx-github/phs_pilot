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
          ,'volume': conf['dbr']['volume']
          ,"target_folder": conf['dbr']['target_folder']}

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

volume = f"/Volumes/{db_catalog}/{volume}"
ddl_path = f"{volume}/{ddl_path}"
target_folder = f"{volume}/{target_folder}"

# string to bool transform
folders = eval(folders)

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,cleanup
# cleanup
_cleanup_catalog (db_catalog,db_schema)
_cleanup_files (target_folder, folders)

print(Fore.YELLOW+'\033[1m'+'Cleanup done!')
