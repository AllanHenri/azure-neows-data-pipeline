from azure.storage.filedatalake import DataLakeServiceClient

from src.config import (
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_FILE_SYSTEM,
    AZURE_DIRECTORY,
    AZURE_FILE_NAME,
    LOCAL_RAW_PATH,
)


def download_file_from_adls() -> str:
    if not all([
        AZURE_STORAGE_CONNECTION_STRING,
        AZURE_FILE_SYSTEM,
        AZURE_DIRECTORY,
        AZURE_FILE_NAME,
    ]):
        raise ValueError("Azure environment variables are incomplete.")

    service_client = DataLakeServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    file_system_client = service_client.get_file_system_client(
        file_system=AZURE_FILE_SYSTEM
    )

    directory_client = file_system_client.get_directory_client(AZURE_DIRECTORY)
    file_client = directory_client.get_file_client(AZURE_FILE_NAME)

    download = file_client.download_file()
    file_bytes = download.readall()

    LOCAL_RAW_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(LOCAL_RAW_PATH, "wb") as f:
        f.write(file_bytes)

    print(f"Downloaded file to: {LOCAL_RAW_PATH}")
    return str(LOCAL_RAW_PATH)


if __name__ == "__main__":
    download_file_from_adls()