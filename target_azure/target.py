"""Azure Storage target class."""

from __future__ import annotations
from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_azure.sinks import TargetAzureBlobSink

class TargetAzureStorage(Target):
    """Sample target for Azure Storage."""

    name = "target-azure"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "storage_account_name",
            th.StringType,
            required=True,
            description="Azure storage account name.",
        ),
        th.Property(
            "storage_account_key",
            th.StringType,
            description="Azure storage account key.",
        ),
        th.Property(
            "naming_convention",
            th.StringType,
            description="""
                The format of the file location. Use {stream}, {date}, {time} and {timestamp} as variables.
                You can also define different file formats: 'parquet', 'json', 'jsonlines' and 'csv'.
            """,
        ),
        th.Property(
            "container_name",
            th.StringType,
            required=True,
            description="The Azure Blob Storage container name where the files will be stored."
        )
    ).to_dict()

    default_sink_class = TargetAzureBlobSink

if __name__ == "__main__":
    TargetAzureStorage.cli()

