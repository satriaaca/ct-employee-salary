import logging
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
from datetime import datetime
from airflow.decorators import dag
from astro.files import File
from astro import sql as aql
from astro.sql.table import Table, Metadata
import sqlalchemy


task_logger = logging.getLogger("airflow.task")

EMPLOYEE_FILEPATH = "datasets/employees.csv"
TIMESHEETS_FILEPATH = "datasets/timesheets.csv"
CONNECTION_ID = "airflow_db"

profile_config = ProfileConfig(
    profile_name="employee_salary",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": "dbt"},
    ),
)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def employee_salary_dags():
    create_schema = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="airflow_db",
        sql="""
        CREATE SCHEMA IF NOT EXISTS dbt;
        DROP TABLE IF EXISTS dbt.raw_employees CASCADE;
        DROP TABLE IF EXISTS dbt.raw_timesheets CASCADE;
        
        """,
    )

    employee_table = aql.load_file(
        input_file=File(EMPLOYEE_FILEPATH),
        output_table=Table(
            name="raw_employees",
            conn_id=CONNECTION_ID,
            metadata=Metadata(
                schema="dbt",
            ),
            columns=[
                sqlalchemy.Column("employee_id", sqlalchemy.Integer, nullable=False),
                sqlalchemy.Column("branch_id", sqlalchemy.Integer, nullable=False),
                sqlalchemy.Column("salary", sqlalchemy.Float, nullable=True),
                sqlalchemy.Column("join_date", sqlalchemy.Text, nullable=True),
                sqlalchemy.Column("resign_date", sqlalchemy.Text, nullable=True),
            ],
        ),
    )

    timesheets_table = aql.load_file(
        input_file=File(TIMESHEETS_FILEPATH),
        output_table=Table(
            name="raw_timesheets",
            conn_id=CONNECTION_ID,
            metadata=Metadata(
                schema="dbt",
            ),
            columns=[
                sqlalchemy.Column("timesheet_id", sqlalchemy.Integer, nullable=False),
                sqlalchemy.Column("employee_id", sqlalchemy.Integer, nullable=False),
                sqlalchemy.Column("date", sqlalchemy.Text, nullable=True),
                sqlalchemy.Column("checkin", sqlalchemy.Text, nullable=True),
                sqlalchemy.Column("checkout", sqlalchemy.Text, nullable=True),
            ],
        ),
    )

    dbt_dags = DbtTaskGroup(
        project_config=ProjectConfig(
            "/usr/local/airflow/dags/dbt",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        operator_args={
            "full_refresh": True,  # used only in dbt commands that support this flag
        },
        default_args={"retries": 2},
    )

    create_schema >> employee_table >> dbt_dags
    create_schema >> timesheets_table >> dbt_dags

employee_salary_dags()
