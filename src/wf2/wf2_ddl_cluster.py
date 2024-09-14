# Databricks notebook source
# DBTITLE 1,imports
# MAGIC %run ./../wf_common/f_common

# COMMAND ----------

# DBTITLE 1,remove wirgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,parameters
# widget
dbutils.widgets.dropdown('clustering','False',['True','False'],"LIQID CLUSTERING")

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
          ,"target_folder": conf['dbr']['target_folder']
          ,"clustering": dbutils.widgets.get('clustering') }

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

volume = f"/Volumes/{db_catalog}/{volume}"
ddl_path = f"{volume}/{ddl_path}"
target_folder = f"{volume}/{target_folder}"

# string to bool transform
clustering = eval(clustering)
folders = ['index']

for folder in folders:
  exec(f"{folder}_path='{ddl_path}/{folder}'")
  exec(f"params['{folder}_path']='{ddl_path}/{folder}'")

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,clustering
files = [x.path for x in dbutils.fs.ls(index_path)]

# indexes
indexes = []
for file in files:
  df = spark.read.text(file,lineSep=";").where("value not ilike '%drop%'")
  for i in df.collect():
    indexes.append(i[0].lower().split(' on ')[-1].replace('\n',''))

dct = {}
for index in indexes:
  if index != '':
    
    tbl = index.split(' using')[0].split('.')[-1]
    
    flds = index.split(' btree ')[-1].replace('(', '').replace(')', '').split(' ')
    if 'text_pattern_ops' in flds:
      flds.remove('text_pattern_ops')

    if tbl not in dct.keys():
      dct[tbl] = flds
    else:
      dct[tbl] = dct[tbl]+flds

fldr = _create_ddl_folder (target_folder,'index')

for t,f in dct.items():
  with open(f"{fldr}/cluster_{t}.sql", "w") as fw:    
    
    set_query = f"alter table {db_catalog}.{db_schema}.{t} set tblproperties ('delta.enabledeletionvectors' = false);\n"
    fw.write(set_query)
    if clustering: spark.sql(set_query)

    cluster_query = f"alter table {db_catalog}.{db_schema}.{t} cluster by ({','.join(f)});\n"
    fw.write(cluster_query)
    if clustering: spark.sql(cluster_query)

    optimize_query = f"optimize {db_catalog}.{db_schema}.{t};\n"
    fw.write(optimize_query)
    if clustering: spark.sql(optimize_query)

  print(f"Clustered: {Fore.YELLOW+t+Fore.WHITE} by {','.join(f)}")

print(Fore.GREEN+'\033[1m'+'Done clustering tables!')
