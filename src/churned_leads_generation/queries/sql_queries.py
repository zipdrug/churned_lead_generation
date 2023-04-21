INSERT_LEAD_CHURNS_DATA_SQL= """
insert into lead_churns (first_name,last_name,patient_id,assigned_user_id,filtered_out,filtered_by_user_id,created_at,updated_at,deleted_at)

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
'182' as assigned_user_id,
null as filtered_out,
null as filtered_by_user_id,
now() as created_at,
now() as updated_at,
null as deleted_at
from 
model_update mu
join patients_ext p on p.patient_id= mu.patient_id
join job_cnt jc on mu.created_at > jc.last_extract_dt
where mu.created_at > jc.last_extract_dt
and mu.enroll_status = p.enroll_status
"""