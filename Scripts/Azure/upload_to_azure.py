from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

account_name = "dataintensiveproject"
account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = "retail-data"

# Raw file you're cleaning
local_file_path = "/Users/rishabhsaudagar/Desktop/NCI CLG/SEMESTER 2/Data Intensive Scalable System/OnlineEcommerce/Data/Raw/online_retail.parquet"  # Replace with your actual local path 
blob_path = "raw/online_retail.parquet"

connect_str = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

with open(local_file_path, "rb") as data:
    container_client.upload_blob(name=blob_path, data=data, overwrite=True)

print(f"Uploaded raw data to Azure: {blob_path}")
