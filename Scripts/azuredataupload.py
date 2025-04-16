from azure.storage.blob import BlobServiceClient, ContentSettings
import os
import os

# TODO: Replace with your values
account_name = "dataintensiveproject"
account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = "retail-data"

# Connect to the Blob service
connect_str = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Path to your local file (adjust as needed)
local_file_path = "/Users/rishabhsaudagar/Desktop/NCI CLG/SEMESTER 2/Data Intensive Scalable System/OnlineEcommerce/Data/Raw/online_retail.parquet"
blob_file_name = "transformed/invoice_summary.parquet"  # This is the blob name (key)

# Upload the file
with open(local_file_path, "rb") as data:
    container_client.upload_blob(
        name=blob_file_name,
        data=data,
        overwrite=True,
        content_settings=ContentSettings(content_type='application/octet-stream')
    )

print(f" Upload complete. File now in: {container_name}/{blob_file_name}")
