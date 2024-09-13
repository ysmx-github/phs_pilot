# Databricks notebook source
# DBTITLE 1,imports
# MAGIC %run ./f_common_premiere

# COMMAND ----------

# DBTITLE 1,remove wirgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,parameters
# widget
dbutils.widgets.text('yaml_file','',"CONFIG FILE")
dbutils.widgets.text('patient_table','patient',"PATIENT TABLE")
dbutils.widgets.text('patient_table_path','',"TABLE PATH")

# config yaml file
yaml_file = dbutils.widgets.get('yaml_file')

conf = _get_config(yaml_file)

# params dict
params = { 'yaml_file': yaml_file
          ,'patient_table': dbutils.widgets.get('patient_table')
          ,"patient_table_path": dbutils.widgets.get('patient_table_path')
          ,"db_catalog": conf['dbr']['db_catalog']          
          ,"db_schema": conf['dbr']['db_schema']
          ,"mode": conf['dbr']['mode'] } 

# create params  
for k, v in params.items():
  exec(f'{k}="{v}"')

# print result
print('Parameters:')
for k in params: print(f'\t{k} - ',eval(k), eval(f'type({k})'))

# COMMAND ----------

# DBTITLE 1,load patient table
# fully qualified table name
patient_table_fq =  f"{db_catalog}.{db_schema}.{patient_table}"

# schema
dfd = spark.sql(f"""select column_name
                          ,data_type
                      from {db_catalog}.information_schema.columns 
                      where table_catalog = '{db_catalog}'             
                        and table_schema = '{db_schema}' 
                        and table_name = 'patient' 
                      order by ordinal_position
                    """)

sch = ','.join([' '.join([x.column_name,'string']) for x in dfd.collect()])+', dummy string'
# print(sch)

# data
df = (spark
      .read
      .text(patient_table_path)
      .where("value <> '' and value not ilike 'cat%'") 
      .withColumn('value', F.split(F.col('value'),'\^A')))

# df.limit(1).display()

data = [x.value for x in df.collect()]

# print(data[0])

# create dataframe for write
dfr = spark.createDataFrame(data,sch).drop('dummy')

# cast fields
sch_dict = {}
for x in dfd.collect():
  sch_dict [x.column_name] = x.data_type

for f in dfr.schema.names:
  dfr = dfr.withColumn(f,F.col(f).cast(sch_dict[f]))

# dfr.limit(5).display() 

# write to table
spark.sql('set spark.sql.ansi.enabled=false')
dfr.write.mode(mode).insertInto(patient_table_fq)

# read back
spark.table(patient_table_fq).display()

print(Fore.GREEN+'\033[1m'+'Loaded patient table!')
