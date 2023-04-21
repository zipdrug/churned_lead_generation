import boto3
import os
import pandas as pd
import load
from utility.db import make_engine
from utility.utils import parse_envs, create_logger
#from load import insert_lead_churns, upd_inc_job_cntl


os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
env_name, environment_secrets = parse_envs()
print('Environment is:', env_name)
print('environment_secrets:', environment_secrets)
DB_ENV: str = environment_secrets["DB_ENV"]

logger = create_logger(logger_name="Churned-Leads", log_group_name="Churned-Leads")

def execute():
    try:
        logger.info("Starting Pharmacy Status process")

        # Create engine.
        engine = make_engine(db_env=DB_ENV)

        # Insert model_updates
        load.insert_lead_churns(engine=engine)

    except Exception as e:
        logger.error(f"EXCEPTION! {e}")


if __name__ == "__main__":
    execute()
