import pandas as pd
import json
from datetime import datetime
from queries.sql_queries import GET_CHURNED_LEADS_DATA_SQL, CHECK_EXISTING_PATIENT_SQL,UPDATE_LEAD_CHURNS_SQL,  INSERT_LEAD_CHURNS_SQL, UPDATE_INCREMENTAL_CONTROL_TABLE_SQL


# Function to fetch Churned Leads Data, Check if already exists in table then update if not Make a new entry into lead_churns
def insert_lead_churns(engine) -> None:
    print("Checking if patient_id already exists in lead_churns table...")
    query = GET_CHURNED_LEADS_DATA_SQL
    patient_id_df = pd.read_sql(sql=query, con=engine)
    len_patient_id_df = len(patient_id_df)

    if len_patient_id_df > 0:
        print("Checking if patient_id's already exists in table...")
        for ind in patient_id_df.index:
            print("loop:", ind, "patient_id:", patient_id_df['patient_id'][ind])
            check_query = CHECK_EXISTING_PATIENT_SQL.format(patient_id=patient_id_df['patient_id'][ind])
            exist_df = pd.read_sql(sql=check_query, con=engine)
            if len(exist_df) > 0:
                print("Patient_id already exists in table, updating..")
                update_query = UPDATE_LEAD_CHURNS_SQL.format(patient_id=patient_id_df['patient_id'][ind])
                conn = engine.connect()
                conn.execute(update_query)
            else:
                print("New entry for churned leads")
                insert_query = INSERT_LEAD_CHURNS_SQL.format(fname=patient_id_df['first_name'][ind], lname=patient_id_df['last_name'][ind], patient_id=patient_id_df['patient_id'][ind])
                conn = engine.connect()
                conn.execute(insert_query)
        #return None

    else:
        print("No entries to insert for churned leads..")


# Function to make job run entry into incremental_job_cntl
def upd_inc_job_cntl(engine) -> None:
    job_type = 'churned_lead_generation'
    des = 'Incremental load extract for Churned Lead Generation'
    flag = 'Y'
    query = UPDATE_INCREMENTAL_CONTROL_TABLE_SQL.format(job_type=job_type, description=des, flag=flag)

    with engine.begin() as conn:  # TRANSACTION
        print("Updating incremental job control table")
        conn.execute(query)
