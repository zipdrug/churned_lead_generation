GET_CHURNED_LEADS_DATA_SQL = """
--insert into lead_churns (first_name,last_name,patient_id,assigned_user_id,filtered_out,filtered_by_user_id,created_at,updated_at,deleted_at)
with job_cnt as (
select
max(last_extract_dt) as last_extract_dt
from
incremental_job_cntl ijc
where
job_type = 'churned_lead_generation'
and flag = 'Y')

,patients_ext as (
select 
p.first_name,
p.last_name,
p.id as patient_id,
cast(p.enroll_status as varchar(255)) as enroll_status,
p.updated_at as updated_at
from patients p 
--where p.enroll_status in ('churned')
)

,model_update as (
SELECT 
patient_id,
created_at,
updated_at, 
Replace(cast(new_fields -> 'enroll_status' AS varchar(255)),'"','')  as enroll_status
from
model_updates mu 
where
cast (new_fields -> 'enroll_status' as varchar(255)) = '"churned"'
)


select 
first_name,
last_name,
p.patient_id as patient_id,
null as assigned_user_id,
null as filtered_out,
null as filtered_by_user_id,
l.payer_id as payer_id,
l.state  as state,
l.plan_type as plan_type,
now() as created_at,
now() as updated_at,
null as deleted_at
from 
model_update mu
join patients_ext p on p.patient_id= mu.patient_id
join leads l on l.patient_id =p.patient_id
join job_cnt jc on mu.created_at > jc.last_extract_dt
where mu.created_at > jc.last_extract_dt
and mu.enroll_status = p.enroll_status
"""

CHECK_EXISTING_PATIENT_SQL = """
SELECT PATIENT_ID FROM LEAD_CHURNS LC WHERE PATIENT_ID = '{patient_id}'
"""

UPDATE_LEAD_CHURNS_SQL = """ UPDATE LEAD_CHURNS SET CREATED_AT = NOW(), UPDATED_AT = NOW()  WHERE PATIENT_ID = '{patient_id}' """

INSERT_LEAD_CHURNS_SQL = """
INSERT INTO LEAD_CHURNS (FIRST_NAME,LAST_NAME,PATIENT_ID,ASSIGNED_USER_ID,FILTERED_OUT,FILTERED_BY_USER_ID,PLAN_TYPE, STATE,PAYER_ID,CREATED_AT,UPDATED_AT,DELETED_AT,CHURNED_OUTREACH_DATE)
VALUES ('{fname}','{lname}','{patient_id}',NULL,NULL, NULL,'{plantype}', '{state}', '{payerid}', NOW(),NOW(),NULL, NULL) """


UPDATE_INCREMENTAL_CONTROL_TABLE_SQL = """
INSERT INTO incremental_job_cntl(job_type,last_extract_dt,description,created_at,flag) VALUES ('{job_type}',now(),'{description}',now(),'{flag}')
"""