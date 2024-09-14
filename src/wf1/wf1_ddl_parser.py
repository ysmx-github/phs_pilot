# Databricks notebook source
# DBTITLE 1,imports
# MAGIC %run ./../wf_common/f_common

# COMMAND ----------

# DBTITLE 1,remove wirgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,parameters
# widget
dbutils.widgets.dropdown('cleanup','False',['True','False'],"CLEANUP REQUIRED")
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
          ,'volume': conf['dbr']['volume']
          ,"mode": conf['dbr']['mode']
          ,"target_folder": conf['dbr']['target_folder']
          ,"cleanup": dbutils.widgets.get('cleanup')
          ,"clustering": dbutils.widgets.get('clustering') }

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

ddl_path = f"{volume}/{ddl_path}"
target_folder = f"{volume}/{target_folder}"

# string to bool transform
cleanup = eval(cleanup)
clustering = eval(clustering)
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
if cleanup:
  
  _cleanup_catalog (db_catalog,db_schema)
  _cleanup_files (target_folder, folders)

  print(Fore.YELLOW+'\033[1m'+'Cleanup done!')
else:
  print(Fore.YELLOW+'\033[1m'+'Cleanup not required!')

# COMMAND ----------

# DBTITLE 1,tables

# create objects
spark.sql(f'create schema if not exists {db_catalog}.{db_schema}')
fldr = _create_ddl_folder(target_folder,'table')

# cretate tables
alter_dict = {}
for file in [x.path for x in dbutils.fs.ls(table_path)]:

  table = file.split('/')[-1].split('.')[0]

  print(f"Table: \t{Fore.YELLOW+table+Fore.WHITE}")

  df = spark.read.text(file,lineSep=";")
  df_create = df.filter(F.col("value").ilike("%create table%"))
  df_alter = df.filter(F.col("value").ilike("%alter%"))

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
  if not cleanup:
    sql_drop = sql_create.split('(')[0].lower().replace("create table","drop table if exists")
    # print(sql_drop)
    spark.sql(sql_drop)
    print(f'\ttable {Fore.YELLOW+table+Fore.WHITE} dropped...' )
  
  # create table
  spark.sql(sql_create)
  print(f'\ttable {Fore.YELLOW+table+Fore.WHITE} created...\n' )

  # prepare alter statements
  if df_alter.count() > 0:
    alter_list = []
    for q in [x.value for x in df_alter.collect()]:
      alter_sql = q.lower().replace(f'{source_schema}.',f'{db_catalog}.{db_schema}.')
      alter_list.append(alter_sql)
    alter_dict[table] = alter_list

  # write databricks ddl to file
  _write_ddl(fldr,table,sql_create)

print(Fore.GREEN+'\033[1m'+'Done creating tables!')


# COMMAND ----------

# DBTITLE 1,alter tables
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

  if not cleanup:
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
