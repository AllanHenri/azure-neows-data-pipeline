import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

def download_file_from_adls() -> str:
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    file_system_name = os.getenv("AZURE_FILE_SYSTEM")
    directory_name = os.getenv("AZURE_DIRECTORY")
    file_name = os.getenv("AZURE_FILE_NAME")
    local_raw_path = os.getenv("LOCAL_RAW_PATH", "data/raw/neo_feed.json")

    if not all([account_name, account_key, file_system_name, directory_name, file_name]):
        raise ValueError("Variáveis de ambiente da Azure estão incompletas.")
    
    account_url = f"https://{account_name}.dfs.core.windows.net"

    service_client = DataLakeServiceClient(account_url=account_url, credential=account_key)

    file_system_client = service_client.get_file_system_client(file_system=file_system_name)

    directory_name = file_system_client.get_directory_client(directory_name)

    file_client = directory_name.get_file_client(file_name)

    download = file_client.download_file()
    file_bytes = download.readall()

    local_path = Path(local_raw_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    with open(local_path, "wb") as f:
        f.write(file_bytes)

    print(f"Arquivo baixado para: {local_path}")
    return str(local_path)

if __name__ == "__main__":
    download_file_from_adis()
    