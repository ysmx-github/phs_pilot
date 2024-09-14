-- Databricks notebook source
-- DBTITLE 1,setup widgets
create widget text catalog default '';
create widget text target_folder default '';

-- COMMAND ----------

-- DBTITLE 1,remove widget
-- remove widget catalog;
-- remove widget target_folder;

-- COMMAND ----------

-- DBTITLE 1,create catalog
create catalog if not exists $catalog;

-- COMMAND ----------

-- DBTITLE 1,create schemas
create schema if not exists $catalog.clinical_raw;
create schema if not exists $catalog.clinical_bronze;
create schema if not exists $catalog.clinical_silver;

-- COMMAND ----------

-- DBTITLE 1,create volume

create volume if not exists $catalog.clinical_raw.clinical_data_volume;


-- COMMAND ----------

-- DBTITLE 1,create target folder
-- MAGIC %python
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get('catalog')
-- MAGIC target_folder = dbutils.widgets.get('target_folder')
-- MAGIC
-- MAGIC target_folder_full = f'/Volumes/{catalog}/clinical_raw/clinical_data_volume/{target_folder}/'
-- MAGIC
-- MAGIC print(dbutils.fs.mkdirs(target_folder_full), dbutils.fs.ls(f'/Volumes/{catalog}/clinical_raw/clinical_data_volume/dbr_ddl_clinical'))
-- MAGIC
-- MAGIC print('Created folder:', target_folder_full)

-- COMMAND ----------

-- DBTITLE 1,grants
-- grant all privileges on catalog $catalog to edm_adm_grp;
-- grant all privileges on schema $catalog.clinical_raw to edm_adm_grp;
-- grant all privileges on schema $catalog.clinical_bronze  to edm_adm_grp;
-- grant all privileges on schema $catalog.clinical_silver to edm_adm_grp;

-- grant usage on catalog $catalog to edm_etl_grp;
-- grant all privileges on schema $catalog.clinical_raw to edm_etl_grp;
-- grant all privileges on schema $catalog.clinical_bronze  to edm_etl_grp;
-- grant all privileges on schema $catalog.clinical_silver to edm_etl_grp;

-- grant usage on catalog $catalog to edm_etl_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema $catalog.clinical_raw to edm_rw_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema $catalog.clinical_bronze to edm_rw_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema $catalog.clinical_silver to edm_rw_grp;

-- COMMAND ----------

-- DBTITLE 1,drop catalog
-- drop catalog $catalog cascade;  
