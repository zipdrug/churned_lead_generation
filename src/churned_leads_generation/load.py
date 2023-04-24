import pandas as pd
import json
from datetime import datetime
from queries.sql_queries import INSERT_LEAD_CHURNS_DATA_SQL, UPDATE_INCREMENTAL_CONTROL_TABLE_SQL, CHECK_EXISTING_PATIENT_SQL, GET_PATIENT_ID_SQL

def insert_lead_churns(engine):
    print("Checking if patient_id already exists in lead_churns table...")
    query = GET_PATIENT_ID_SQL
    patient_id_df = pd.read_sql(sql=query, con=engine)
    print("patient_id_df", patient_id_df)
    len_patient_id_df = len(patient_id_df)

    if len_patient_id_df > 0:
        #patient_id_df = pd.read_sql(sql=query, con=engine)
        print("Checking if patient_id's already exists in table...")
        for ind in patient_id_df.index:
            print("loop:", ind)
            print("value", patient_id_df.iloc[[ind]], patient_id_df['patient_id'][ind])
            check_query = CHECK_EXISTING_PATIENT_SQL.format(patient_id=patient_id_df['patient_id'][ind])
            exist_df = pd.read_sql(sql=check_query, con=engine)
            if len(exist_df) > 0:
                print("Patient_id already exists in table")
                print("Updating values..")
                pass
        return None

    else:
        print("New inserts..")
    print("Churned leads Insert")
    conn = engine.connect()
    conn.execute(INSERT_LEAD_CHURNS_DATA_SQL)
    return patient_id_df

def upd_inc_job_cntl(engine) -> None:
    job_type = 'churned_lead_generation'
    des = 'Incremental load extract for Churned Lead Generation'
    flag = 'Y'
    query = UPDATE_INCREMENTAL_CONTROL_TABLE_SQL.format(job_type=job_type, description=des, flag=flag)
    #query = "INSERT INTO incremental_job_cntl(job_type,last_extract_dt,description,created_at,flag) VALUES (%s,%s,%s,%s,%s)"

    with engine.begin() as conn:  # TRANSACTION
        print("Updating incremental job control table")
        conn.execute(query)
        #conn.execute(UPDATE_INCREMENTAL_CONTROL_TABLE_SQL, (job_type, datetime.now(), des, datetime.now(), flag))
        #conn.execute(query)