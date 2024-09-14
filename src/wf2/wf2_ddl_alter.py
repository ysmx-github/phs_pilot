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
folders = ['table']

for folder in folders:
  exec(f"{folder}_path='{ddl_path}/{folder}'")
  exec(f"params['{folder}_path']='{ddl_path}/{folder}'")

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))


# COMMAND ----------

# DBTITLE 1,prepare alter dictionary
# create objects
fldr = _create_ddl_folder(target_folder,'table')

# cretate tables
alter_dict = {}
for file in [x.path for x in dbutils.fs.ls(table_path)]:

  table = file.split('/')[-1].split('.')[0]

  df = spark.read.text(file,lineSep=";")
  df_alter = df.filter(F.col("value").ilike("%alter%"))

  # prepare alter statements
  if df_alter.count() > 0:
    alter_list = []
    for q in [x.value for x in df_alter.collect()]:
      alter_sql = q.lower().replace(f'{source_schema}.',f'{db_catalog}.{db_schema}.')
      alter_list.append(alter_sql)
    alter_dict[table] = alter_list

# COMMAND ----------

# DBTITLE 1,alter tables
# create objects
fldr = _create_ddl_folder(target_folder,'table')

# get list of existing constraints
constraint_sql = f"""select constraint_name
                       from {db_catalog}.information_schema.constraint_table_usage
                      where table_catalog = '{db_catalog}' 
                        and table_schema = '{db_schema}'
                        and constraint_catalog = '{db_catalog}'  
                        and constraint_schema = '{db_schema}' 
                  """

constraints = [x.constraint_name for x in spark.sql(constraint_sql).collect()]  

# alter tables
query_list = []
for table in alter_dict.keys():

  for alter_sql in alter_dict[table]:

    fk = [x for x in alter_sql.split(" ") if 'fk' in x][0]
    new_fk = fk    

    if fk in constraints:
      new_fk = f'{table}_{fk}'
      alter_sql = alter_sql.replace(fk,new_fk)
    
    constraints.append(new_fk)
    
    # print(alter_sql)
    spark.sql(alter_sql)

    query_list.append(alter_sql)

    print(f'Altered table {Fore.YELLOW+table+Fore.WHITE} - added constraint {Fore.YELLOW+new_fk+Fore.WHITE}...' )

# write all alter ddl statements to one file
_write_ddl(fldr,'alter_statements','',query_list)

print(Fore.GREEN+'\033[1m'+'Done altering tables!')
