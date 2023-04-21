import pandas as pd
import json
from queries.sql_queries import INSERT_LEAD_CHURNS_DATA_SQL


class ChurnedLeads:
    def __init__(self):
        self.engine = engine
        self.job_type = 'churned_lead_generation'
        self.extract_date = extract_date
        self.des = 'Incremental load extract for Churned Lead Generation'
        self.flag = 'Y'

    def insert_lead_churns(self, engine):
        print("Churned leads Insert")
        conn = engine.connect()
        conn.execute(INSERT_LEAD_CHURNS_DATA_SQL)
        return None

    def upd_inc_job_cntl(self) -> None:
        query = "INSERT INTO incremental_job_cntl(job_type,last_extract_dt,description,created_at,flag) VALUES (%s,%s,%s,%s,%s)"
        engine = self.engine()
        with engine.begin() as conn:  # TRANSACTION
            #print("insert")
            conn.execute(query, (self.job_type, self.extract_date, self.des, datetime.now(), self.flag,))
            #conn.execute(query)