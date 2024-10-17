# Databricks notebook source


# Define the file path
file_path = "abfss://demo@entrepoetldbwnp3x51.dfs.core.windows.net/patient_ins.AVERA_snappy.parquet"

# Read the Parquet file into a DataFrame
patient_df = spark.read.parquet(file_path)



# COMMAND ----------

# Rename the 'newline' column to 'delta_flg'
patient_df = patient_df.withColumnRenamed("newline", "delta_flg")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG entrepo_dev;
# MAGIC USE SCHEMA edm_mh90_dev;

# COMMAND ----------

# Create a temporary view from the DataFrame
patient_df.createOrReplaceTempView("temp_patient")

# Perform the merge operation
spark.sql("""
MERGE INTO patient AS target
USING temp_patient AS source
ON target.patient_sk = source.patient_sk
WHEN MATCHED AND source.delta_flg != 'I' THEN
  UPDATE SET
    target.patient_bk = source.patient_bk,
    target.record_batch_audit_id = source.record_batch_audit_id,
    target.record_insert_timestamp = source.record_insert_timestamp,
    target.record_insert_id = source.record_insert_id,
    target.record_update_timestamp = source.record_update_timestamp,
    target.record_update_id = source.record_update_id,
    target.job_run_id = source.job_run_id,
    target.active_patient_ind = source.active_patient_ind,
    target.vendor_code = source.vendor_code,
    target.data_source_code = source.data_source_code,
    target.client_code = source.client_code,
    target.medical_record_number = source.medical_record_number,
    target.master_person_identifier = source.master_person_identifier,
    target.birth_datetime = source.birth_datetime,
    target.patient_family_name = source.patient_family_name,
    target.patient_given_name = source.patient_given_name,
    target.patient_middle_name = source.patient_middle_name,
    target.patient_name = source.patient_name,
    target.administrative_sex_code = source.administrative_sex_code,
    target.administrative_sex_code_descr = source.administrative_sex_code_descr,
    target.sexual_identity_code = source.sexual_identity_code,
    target.sexual_identity_descr = source.sexual_identity_descr,
    target.race_code = source.race_code,
    target.race_descr = source.race_descr,
    target.gender_identity_code = source.gender_identity_code,
    target.gender_identity_descr = source.gender_identity_descr,
    target.marital_status_code = source.marital_status_code,
    target.ethnic_group_code = source.ethnic_group_code,
    target.ethnicity_code = source.ethnicity_code,
    target.ethnicity_descr = source.ethnicity_descr,
    target.religion_code = source.religion_code,
    target.language_code = source.language_code,
    target.vip_indicator = source.vip_indicator,
    target.veteran_status_code = source.veteran_status_code,
    target.citizenship_name = source.citizenship_name,
    target.birthplace_name = source.birthplace_name,
    target.mothers_name = source.mothers_name,
    target.mother_maiden_name = source.mother_maiden_name,
    target.multiple_birth_indicator = source.multiple_birth_indicator,
    target.multiple_birth_order = source.multiple_birth_order,
    target.death_indicator = source.death_indicator,
    target.death_datetime = source.death_datetime,
    target.addr_type = source.addr_type,
    target.addr_line1 = source.addr_line1,
    target.addr_line2 = source.addr_line2,
    target.city_name = source.city_name,
    target.state_name = source.state_name,
    target.postal_code = source.postal_code,
    target.country_name = source.country_name,
    target.employer_code = source.employer_code,
    target.employer_name = source.employer_name,
    target.employment_status_code = source.employment_status_code,
    target.employment_status_descr = source.employment_status_descr,
    target.source_update_timestamp = source.source_update_timestamp,
    target.practitioner_sk = source.practitioner_sk
WHEN NOT MATCHED AND source.delta_flg = 'I' THEN
  INSERT (
    patient_sk, patient_bk, record_batch_audit_id, record_insert_timestamp, record_insert_id, 
    record_update_timestamp, record_update_id, job_run_id, active_patient_ind, vendor_code, 
    data_source_code, client_code, medical_record_number, master_person_identifier, birth_datetime, 
    patient_family_name, patient_given_name, patient_middle_name, patient_name, administrative_sex_code, 
    administrative_sex_code_descr, sexual_identity_code, sexual_identity_descr, race_code, race_descr, 
    gender_identity_code, gender_identity_descr, marital_status_code, ethnic_group_code, ethnicity_code, 
    ethnicity_descr, religion_code, language_code, vip_indicator, veteran_status_code, citizenship_name, 
    birthplace_name, mothers_name, mother_maiden_name, multiple_birth_indicator, multiple_birth_order, 
    death_indicator, death_datetime, addr_type, addr_line1, addr_line2, city_name, state_name, postal_code, 
    country_name, employer_code, employer_name, employment_status_code, employment_status_descr, 
    source_update_timestamp, practitioner_sk
  ) VALUES (
    source.patient_sk, source.patient_bk, source.record_batch_audit_id, source.record_insert_timestamp, source.record_insert_id, 
    source.record_update_timestamp, source.record_update_id, source.job_run_id, source.active_patient_ind, source.vendor_code, 
    source.data_source_code, source.client_code, source.medical_record_number, source.master_person_identifier, source.birth_datetime, 
    source.patient_family_name, source.patient_given_name, source.patient_middle_name, source.patient_name, source.administrative_sex_code, 
    source.administrative_sex_code_descr, source.sexual_identity_code, source.sexual_identity_descr, source.race_code, source.race_descr, 
    source.gender_identity_code, source.gender_identity_descr, source.marital_status_code, source.ethnic_group_code, source.ethnicity_code, 
    source.ethnicity_descr, source.religion_code, source.language_code, source.vip_indicator, source.veteran_status_code, source.citizenship_name, 
    source.birthplace_name, source.mothers_name, source.mother_maiden_name, source.multiple_birth_indicator, source.multiple_birth_order, 
    source.death_indicator, source.death_datetime, source.addr_type, source.addr_line1, source.addr_line2, source.city_name, source.state_name, 
    source.postal_code, source.country_name, source.employer_code, source.employer_name, source.employment_status_code, source.employment_status_descr, 
    source.source_update_timestamp, source.practitioner_sk
  )
""")

# COMMAND ----------

