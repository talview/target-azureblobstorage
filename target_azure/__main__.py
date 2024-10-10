"""tap-azure entry point."""

from __future__ import annotations

# from target_azure.target import Targettap-azure

# Targettap-azure.cli()

from target_azure.target import TargetAzureStorage

if __name__ == "__main__":
    TargetAzureStorage.cli()





