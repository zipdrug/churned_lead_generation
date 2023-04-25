import boto3
import os
import pandas as pd
from utility.db import make_engine
from utility.utils import parse_envs, create_logger
from load import insert_lead_churns, upd_inc_job_cntl
import traceback


os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
env_name, environment_secrets = parse_envs()
print('Environment is:', env_name)
print('environment_secrets:', environment_secrets)
DB_ENV: str = environment_secrets["DB_ENV"]

logger = create_logger(logger_name="Churned-Leads", log_group_name="Churned-Leads")

'''
1.  Function insert_lead_churns the records into lead_churns table for only members with Churned enroll status.
2.  Also if member already exists in table and new record comes with same patient_id , record in table is updated.
3.  Function upd_inc_job_cntl logs the job run into table incremental_job_cntl with Job Type: churned_lead_generation
'''

def execute():
    try:
        logger.info("Starting Churned Leads Generation process")

        # Create engine.
        engine = make_engine(db_env=DB_ENV)

        # Insert into lead churns
        insert_lead_churns(engine=engine)

        # Updating job run entry incremental_job_cntl

        upd_inc_job_cntl(engine=engine)

    except Exception as e:
        logger.error(f"EXCEPTION! {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    execute()
