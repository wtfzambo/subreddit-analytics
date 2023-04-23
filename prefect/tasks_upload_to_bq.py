import os
from typing import Literal, cast

import pandas as pd
from dotenv import load_dotenv
from prefect_gcp import GcpCredentials
from utils_ import (
    BQ_DATASET,
    DESCRIBE_TABLE,
    GCP_CREDENTIALS,
    SELECT_STAR_FROM,
    DuckDBManager,
)

from prefect.tasks import task


load_dotenv()


@task(
    name="Read duckdb table",
    task_run_name="Read duckdb table '{table}'",
    log_prints=True,
)
def read_duckdb_table(table: Literal["submissions", "comments"]):
    con = DuckDBManager().duckdb_con

    print(f"Reading duckdb table {table}...")
    df = con.sql(SELECT_STAR_FROM.format(table=table)).df()

    return df


@task(
    name="Write to BigQuery table",
    task_run_name="Write to BigQuery table '{table}'",
    log_prints=True,
)
def write_to_bq(
    df: pd.DataFrame,
    table: Literal["submissions", "comments"],
):
    creds: GcpCredentials = GcpCredentials.load(GCP_CREDENTIALS)  # type: ignore
    print(f"Writing to BQ table {table}...")
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{table}",
        project_id=os.environ["GCP_PROJECT_ID"],
        credentials=creds.get_credentials_from_service_account(),
        if_exists="replace",
    )
