import pandas as pd
import json
from datetime import datetime
from queries.sql_queries import INSERT_LEAD_CHURNS_DATA_SQL

def insert_lead_churns(engine):
    print("Churned leads Insert")
    conn = engine.connect()
    conn.execute(INSERT_LEAD_CHURNS_DATA_SQL)
    return None

def upd_inc_job_cntl(engine) -> None:
    job_type = 'churned_lead_generation'
    des = 'Incremental load extract for Churned Lead Generation'
    flag = 'Y'
    query = "INSERT INTO incremental_job_cntl(job_type,last_extract_dt,description,created_at,flag) VALUES (%s,%s,%s,%s,%s)"

    with engine.begin() as conn:  # TRANSACTION
        #print("insert")
        conn.execute(query, (job_type, datetime.now(), des, datetime.now(), flag))
        #conn.execute(query)