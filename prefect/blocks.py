import os
from pathlib import Path

from dotenv import load_dotenv
from prefect_gcp import GcpCredentials


load_dotenv()

gcp_service_account_path = Path(
    os.environ["GCP_SERVICE_ACCOUNT_FILE_PATH"]
).expanduser()

credentials_block = GcpCredentials(service_account_file=gcp_service_account_path)
credentials_block.save("dataengineering-subreddit")  # type: ignore
