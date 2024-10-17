import os
import pandas as pd
from singer_sdk.sinks import RecordSink
from azure.storage.blob import BlobServiceClient, BlobClient
import re
import logging
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
import atexit

class TargetAzureBlobSink(RecordSink):
    """Azure Storage target sink class for streaming."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob_client = None
        self.local_file_path = None
        self.stream_initialized = False
        self.logger.setLevel(logging.DEBUG)
        atexit.register(self.finalize)

    def start_stream(self) -> None:
        """Initialize the stream."""
        self.logger.info(f"Starting stream for {self.stream_name}")
        account_name = self.config["storage_account_name"]
        account_key = self.config["storage_account_key"]
        container_name = self.config.get("container_name", "default-container")
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        subfolder = self.config.get("subfolder_path", "default_subfolder_path")
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        
        try:
            self.container_client.create_container()
            self.logger.info(f"Created container: {container_name}")
        except ResourceExistsError:
            self.logger.info(f"Container {container_name} already exists.")

        file_name = self.format_file_name()
        self.blob_path = os.path.join(subfolder, file_name)
        self.local_file_path = os.path.join("/tmp", os.path.basename(file_name))
        os.makedirs(os.path.dirname(self.local_file_path), exist_ok=True)

        if not os.path.exists(self.local_file_path):
            with open(self.local_file_path, 'w') as f:
                f.write('')  # Create an empty local file if it doesn't exist.

        self.blob_client = self.container_client.get_blob_client(blob=self.blob_path)
        self.logger.debug(f"Initialized blob client for: {self.blob_path}")
        self.stream_initialized = True

    def process_record(self, record: dict, context: dict) -> None:
        """Process and write the record to Azure Blob Storage."""
        if not self.stream_initialized:
            self.start_stream()

        if not self.local_file_path:
            self.logger.error("local_file_path is None.")
            return

        self.logger.debug(f"Processing record for stream '{self.stream_name}': {record}")
        df = pd.DataFrame([record])
        
        # Ensure correct appending behavior
        header = not os.path.exists(self.local_file_path) or os.path.getsize(self.local_file_path) == 0
        df.to_csv(self.local_file_path, mode='a', index=False, header=header)  # Append record to local file
        self.logger.debug(f"Appended record to local file: {self.local_file_path}")

    def format_file_name(self) -> str:
        """Format the file name based on the context and Azure Blob Storage naming rules."""
        naming_convention = self.config.get("naming_convention", "{stream}.csv")  # Provide a default naming convention
        stream_name = self.stream_name

        file_name = naming_convention.replace("{stream}", stream_name)
        file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)  # Replace or remove invalid characters for Azure Blob Storage

        self.logger.debug(f"Formatted file name: {file_name}")
        return file_name

    def finalize(self) -> None:
        """Upload the local file to Azure Blob Storage and remove it."""
        self.logger.info(f"Finalizing stream for {self.stream_name}")

        if not self.local_file_path:
            self.logger.error("local_file_path is None during finalize.")
            return

        self.logger.debug(f"Preparing to upload {self.local_file_path} to Azure Blob Storage")

        try:
            # Check if file exists before uploading
            if not os.path.exists(self.local_file_path):
                self.logger.error(f"Local file does not exist: {self.local_file_path}")
                return

            with open(self.local_file_path, "rb") as data:
                self.blob_client.upload_blob(data, overwrite=True)
            self.logger.info(f"Successfully uploaded {self.blob_path} to Azure Blob Storage")
        except Exception as e:
            self.logger.error(f"Failed to upload {self.blob_path} to Azure Blob Storage: {e}")
            raise
        finally:
            # Clean up the local file after upload
            if os.path.exists(self.local_file_path):
                os.remove(self.local_file_path)
                self.logger.debug(f"Removed local file: {self.local_file_path}")
            else:
                self.logger.error(f"Local file not found during cleanup: {self.local_file_path}")

        self.logger.info(f"Successfully finalized stream for {self.stream_name}")

if __name__ == "__main__":
    TargetAzureBlobSink.cli()