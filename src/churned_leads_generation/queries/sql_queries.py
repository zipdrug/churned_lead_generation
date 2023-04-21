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
null as assigned_user_id,
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






CHECK_EXISTING_LEADS = """
SELECT patient_id from leads WHERE patient_id = '{}' 
"""

CHECK_EXISTING_LEAD_ARCHIVES = """
SELECT patient_id from lead_archives WHERE patient_id = '{}'
"""

CHECK_ENROLL_STATUS = """
SELECT id, enroll_status  from patients WHERE id = '{}' 
"""

CHECK_LOB = """
SELECT p.plan_type,
        m.member_id,
        m.patient_id
    FROM plans p
    INNER JOIN (
        select
        patient_id,
        member_id,
        plan_id,
            rank()
            over (partition by patient_id order by created_at desc) as rn
        FROM memberships) m
    ON m.plan_id = p.id and m.rn = 1 
    WHERE m.patient_id = '{}'
"""

PATIENT_QUERY = """
SELECT
    p.id as patient_id,
    p.payer_id,
    plans.plan_type,
    p.maintenance_count,
    addresses.postal_code,
    p.program_eligibility,
    ppnt.network_id,
    addresses.state,
    p.language as patient_language
FROM
    patients p
    INNER JOIN (
        select
        patient_id,
        member_id,
        plan_id,
            rank()
            over (partition by patient_id order by created_at desc) as rn
        FROM memberships) m on m.patient_id = p.id
    inner join plans on plans.id = m.plan_id
    INNER JOIN addresses ON p.id = addresses.patient_id AND addresses.order = 0 and addresses.deleted_at is null
    left join plans_pharmacy_network_types ppnt on plans.id = ppnt.plan_id
where
    p.id = '{}'
"""

PHARMACIES_QUERY = """
select
    postal_codes.postal_code,
    postal_codes.payer_id,
    postal_codes.pharmacy_id,
    pnt.lob as plan_type,
    pnt.id as network_id,
    p.maintenance_threshold,
    case
        when zipdrug_service.pharmacy_id is not null AND cpesn_service.pharmacy_id is not NULL then 'zipdrug_and_cpesn_eligible'
        when zipdrug_service.pharmacy_id is not null and cpesn_service.pharmacy_id is null then 'zipdrug_eligible'
        when zipdrug_service.pharmacy_id is null and cpesn_service.pharmacy_id is not null then 'cpesn_eligible'
        else 'zipdrug_eligible'
    end as program_eligibility
from
    postal_codes
    inner join pharmacies p on postal_codes.pharmacy_id = p.id
    inner join pharmacies_network pn on p.id = pn.pharmacy_id
    inner join pharmacy_network_types pnt on pn.pharmacy_network_type_id = pnt.id
    left join ( select
                    pharmacy_id
                from
                    pharmacy_services
                    inner join pharmacy_service_types pst on pharmacy_services.pharmacy_service_type_id = pst.id
                where 
                    pharmacy_service_type = 'Zipdrug'
                    and 
                    pharmacy_services.deleted_at is null
                    and
                    pst.deleted_at is null
               )zipdrug_service on p.id = zipdrug_service.pharmacy_id
    left join ( select
                    pharmacy_id
                from
                    pharmacy_services
                    inner join pharmacy_service_types pst on pharmacy_services.pharmacy_service_type_id = pst.id
                where 
                    pharmacy_service_type = 'CPESN (Community Pharmacy Enhanced Services Network)'
                    and 
                    pharmacy_services.deleted_at is null
                    and
                    pst.deleted_at is null
               )cpesn_service on p.id = cpesn_service.pharmacy_id
where
    p.status = 'active'
    AND
    pn.deleted_at IS NULL
    AND
    pnt.deleted_at IS NULL
"""

ARCHIVE_LEAD_QUERY = """
INSERT INTO lead_archives (id, cadence, patient_id, member_id, payer_id, plan_type, potential_pharmacy_id, lead_status,
days_left_to_call, team_lead_user_id, expansion_group, assigned_dsa_user_id, updated_at, created_at, deleted_at,
call_after_date, state, patient_language, archival_reason, archival_date, special_notes)
SELECT DISTINCT l.id,
l.cadence,
l.patient_id,
l.member_id,
l.payer_id,
l.plan_type,
l.potential_pharmacy_id,
l.lead_status,
l.days_left_to_call,
l.team_lead_user_id,
l.expansion_group,
l.assigned_dsa_user_id,
l.updated_at,
l.created_at,
l.deleted_at,
l.call_after_date,
l.patient_language,
l.state,
'{}' as archival_reason,
now() as archival_date,
l.special_notes
FROM leads l
where l.patient_id = {}
"""

DELETE_LEAD_QUERY = "DELETE FROM leads WHERE leads.patient_id = {}"

UPDATE_LEAD_QUERY = 'UPDATE leads SET potential_pharmacy_id = {}, updated_at = now()'
