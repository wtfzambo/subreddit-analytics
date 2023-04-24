import os
from pathlib import Path

from dotenv import load_dotenv
from prefect_gcp import GcpCredentials
from utils_ import GCP_CREDENTIALS


load_dotenv()

gcp_service_account_path = Path(os.environ["GCP_SERVICE_ACCOUNT_FILE_PATH"])

credentials_block = GcpCredentials(service_account_file=gcp_service_account_path)
credentials_block.save(GCP_CREDENTIALS, overwrite=True)  # type: ignore
