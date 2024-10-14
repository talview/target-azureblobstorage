import pandas as pd
from singer_sdk.sinks import RecordSink
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import os
import logging
from datetime import datetime

class TargetAzureBlobSink(RecordSink):
    max_buffer_size = 2000  # Number of records to buffer before uploading

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records_buffer = []
        self.initialize_connection()
        self.ensure_container()

    def initialize_connection(self):
        account_name = self.config["storage_account_name"]
        account_key = self.config["storage_account_key"]
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def ensure_container(self):
        # container_name is now mandatory
        container_name = self.config["container_name"]
        self.container_client = self.blob_service_client.get_container_client(container_name)
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            pass  # Container already exists

    def stream_folder(self):
        root_folder = self.config["root_folder"]
        return os.path.join(root_folder, self.stream_name)

    def format_file_name(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        file_name_template = self.config.get("file_name_template", "{stream}_{timestamp}.csv")
        return file_name_template.format(stream=self.stream_name, timestamp=timestamp)

    def process_record(self, record: dict, context: dict) -> None:
        self.records_buffer.append(record)
        if len(self.records_buffer) >= self.max_buffer_size:
            self.flush_buffer()

    def flush_buffer(self):
        df = pd.DataFrame(self.records_buffer)
        blob_path = os.path.join(self.stream_folder(), self.format_file_name())
        blob_client = self.container_client.get_blob_client(blob=blob_path)
        csv_data = df.to_csv(index=False)
        blob_client.upload_blob(csv_data, blob_type="AppendBlob", overwrite=True)
        self.records_buffer = []  # Clear buffer after uploading

    def finalize(self) -> None:
        if self.records_buffer:
            self.flush_buffer()
        super().finalize()

if __name__ == "__main__":
    TargetAzureBlobSink.cli()