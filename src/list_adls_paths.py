from azure.storage.filedatalake import DataLakeServiceClient

from src.config import AZURE_STORAGE_CONNECTION_STRING, AZURE_FILE_SYSTEM


def run() -> None:
    service_client = DataLakeServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    file_system_client = service_client.get_file_system_client(
        file_system=AZURE_FILE_SYSTEM
    )

    print(f"Listing paths in file system: {AZURE_FILE_SYSTEM}")
    for path in file_system_client.get_paths():
        print(path.name)


if __name__ == "__main__":
    run()