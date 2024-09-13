-----------------------------------------------------
-- script for testing
-----------------------------------------------------

-- update 
update patient set vip_indicator = 1, record_update_timestamp = current_timestamp() where patient_sk = '5240336194590453073';
select * from patient where patient_sk = '5240336194590453073';

-- delete 1
delete from patient where patient_sk = 1078290548366611718;

-- delete 2
create or replace table patient_del as select * from patient where patient_sk = -1262303210275998204;
delete from patient where patient_sk = -1262303210275998204;
insert into patient select * from patient_del where patient_sk = -1262303210275998204;
update patient set record_update_timestamp = current_timestamp() where patient_sk = -1262303210275998204;
drop table patient_del;

--insert
insert into patient select 9204691118228220899+1 patient_sk, patient.* except(patient_sk) from patient where patient_sk = -3922488618293888550;
update patient set record_update_timestamp = current_timestamp() where patient_sk = 9204691118228220899+1;

-----------------------------------------------------
-- validation
-----------------------------------------------------

-- update
select vip_indicator, record_update_timestamp from patient where patient_sk = 5240336194590453073
union all
select vip_indicator, record_update_timestamp from patient_dlt where patient_sk = 5240336194590453073;

-- delete 1
select * from patient where patient_sk = 1078290548366611718
union all
select * from patient_dlt where patient_sk = 1078290548366611718;

-- delete 2
select record_update_timestamp from patient where patient_sk = -1262303210275998204
union all
select record_update_timestamp from patient_dlt where patient_sk = -1262303210275998204;

--insert
select * from patient where patient_sk = 9204691118228220899+1
union all
select * from patient_dlt where patient_sk = 9204691118228220899+1;

---------------------

-- select count(*) cnt from patient;

-- select count (distinct patient_sk)
--       ,count (distinct patient_bk) 
--       ,count (distinct record_insert_timestamp) 
--       ,count (distinct record_update_timestamp) 
--   from patient;
