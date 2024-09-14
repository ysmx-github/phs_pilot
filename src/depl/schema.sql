create catalog if not exists clinical;
create schema if not exists clinical.clinical_raw;
create schema if not exists clinical.clinical_bronze;
create schema if not exists clinical.clinical_silver;

create volume if not exists clinical.clinical_raw.clinical_data_volume;

-- grant all privileges on catalog clinical to edm_adm_grp;
-- grant all privileges on schema clinical.clinical_raw to edm_adm_grp;
-- grant all privileges on schema clinical.clinical_bronze  to edm_adm_grp;
-- grant all privileges on schema clinical.clinical_silver to edm_adm_grp;

-- grant usage on catalog clinical to edm_etl_grp;
-- grant all privileges on schema clinical.clinical_raw to edm_etl_grp;
-- grant all privileges on schema clinical.clinical_bronze  to edm_etl_grp;
-- grant all privileges on schema clinical.clinical_silver to edm_etl_grp;

-- grant usage on catalog databricks_ssa to edm_etl_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema clinical.clinical_raw to edm_rw_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema clinical.clinical_bronze to edm_rw_grp;
-- grant usage, modify, read volume, refresh, select, write volume on schema clinical.clinical_silver to edm_rw_grp;

-- drop catalog clinical cascade;  
