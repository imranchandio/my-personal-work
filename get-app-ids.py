"""
This module interacts with Oracle Cloud Infrastructure (OCI) Data Flow to retrieve existing 
applications within a given compartment and store their IDs in a JSON file.

It utilizes the OCI SDK to list applications in a compartment and handles pagination if there 
are multiple pages of results.

Configuration:
- The module expects the following environment variables to be set:
  - ENVIRONMENT: Specifies the environment.
  - OCI_CONFIG_FILE: Path to the OCI configuration file.
  - COMPARTMENT_ID: The OCI compartment ID to retrieve applications from.
"""
import os
import sys
import json
import oci

def get_existing_applications(compartment_id: str):
    """
    Retrieve existing OCI Data Flow applications from a specified compartment, handling pagination 
    if necessary.

    Args:
        compartment_id (str): The OCID of the compartment from which to list applications.

    Returns:
        dict: A dictionary mapping application display names to their respective OCIDs.
    """
    applications = []
    next_page = None

    while True:
        response = client.list_applications(compartment_id, page=next_page)
        applications.extend(response.data)

        # Check for the next page
        next_page = response.headers.get("opc-next-page")
        if next_page is None:
            break

    return {f"{app.display_name}_id": app.id for app in applications}

environment=os.getenv('ENVIRONMENT')
OCI_CONFIG_PATH = '~/.oci/config'

if environment == "STG":
    oci_config = oci.config.from_file(
    os.getenv('OCI_CONFIG_FILE', OCI_CONFIG_PATH),
    profile_name="STG"
)

elif environment == "DEV":
   oci_config = oci.config.from_file(
   os.getenv('OCI_CONFIG_FILE', OCI_CONFIG_PATH),
   profile_name="DEV")   

else:
    oci_config = oci.config.from_file(
    os.getenv('OCI_CONFIG_FILE', OCI_CONFIG_PATH),
    profile_name="DEFAULT"
)

client = oci.data_flow.DataFlowClient(oci_config)
compartment=os.getenv('COMPARTMENT_ID')
data = get_existing_applications(compartment)
with open('app_ids.json', 'w+', encoding='utf-8') as file:
    json.dump(data, file, indent=4)

print("JSON Created Successfully.")