import json
import os
import requests
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

api_key = os.getenv("NASA_API_KEY")
start_date = os.getenv("START_DATE")
end_date = os.getenv("END_DATE")

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
file_system_name =os.getenv("AZURE_FILE_SYSTEM")
directory_name = os.getenv("AZURE_DIRECTORY")
file_name = os.getenv("AZURE_FILE_NAME")

url = "https://api.nasa.gov/neo/rest/v1/feed"

params = {
    "start_date": start_date,
    "end_date": end_date,
    "api_key": api_key,
}

response = requests.get(url, params=params, timeout=30)
response.raise_for_status()
data = response.json()

service_client= DataLakeServiceClient.from_connection_string(conn_str)
file_system_client = service_client.get_file_system_client(file_system=file_system_name)
directory_client = file_system_client.get_directory_client(directory_name)
directory_client.create_directory()

file_client = directory_client.get_file_client(file_name)
payload = json.dumps(data, indent=2)
file_client.create_file()
file_client.append_data(data=payload, offset=0, length=len(payload))
file_client.flush_data(len(payload))

print("Upload concluído")