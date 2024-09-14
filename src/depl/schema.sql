create catalog if not exists databricks_ssa;
create schema if not exists databricks_ssa.clinical_raw;
create schema if not exists databricks_ssa.clinical_bronze;
create schema if not exists databricks_ssa.clinical_silver;

create volume databricks_ssa.clinical_raw.clinical_data_volume;

-- grant all privileges on catalog databricks_ssa to edm_adm_grp;
-- grant all privileges on schema databricks_ssa.clinical_raw to edm_adm_grp;
-- grant all privileges on schema databricks_ssa.clinical_bronze  to edm_adm_grp;
-- grant all privileges on schema databricks_ssa.clinical_silver to edm_adm_grp;

-- grant usage on catalog databricks_ssa to edm_etl_grp;
-- grant all privileges on schema databricks_ssa.clinical_raw to edm_etl_grp;
-- grant all privileges on schema databricks_ssa.clinical_bronze  to edm_etl_grp;
-- grant all privileges on schema databricks_ssa.clinical_silver to edm_etl_grp;

-- grant usage on catalog databricks_ssa to edm_etl_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema databricks_ssa.clinical_raw to edm_rw_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema databricks_ssa.clinical_bronze to edm_rw_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema databricks_ssa.clinical_silver to edm_rw_grp;

-- drop catalog  databricks_ssa cascade;  
