from typing import Union, List, Dict, Generator

import dlt
from dlt.common import logger
from dlt.sources import DltResource
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials

from .google_drive_api_client import GoogleDriveClient
from datetime import datetime


@dlt.source(name='google_drive')
def google_drive(
    drive_id: str = dlt.config.value,
    folders: List[Dict[str, str]] = dlt.config.value,
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value
) -> Generator[DltResource, None, None]:
    """ Source for Google Drive
    """

    file_ids = []

    client = GoogleDriveClient(credentials)

    # Define dynamic resource function
    def get_files(
            drive_id: str,
            folder_id: str,
            mime_type: str,
            updated_at: str = dlt.sources.incremental(
                "lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z"
            )
    ):
        modified_since = datetime.strptime(updated_at)
        files = client.list_files_in_folder(
            drive_id=drive_id,
            folder_id=folder_id,
            mime_type=mime_type,
            modified_since=modified_since,
        )

        parsing_functions_by_mime_type = {
            "text/csv": client.parse_csv_to_json,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": client.parse_xlsx_to_json
        }

        parse_function = parsing_functions_by_mime_type.get(mime_type)
        if not parse_function:
            raise ValueError(
                f"No parsing function available for MIME type: {mime_type}")

        for file in files:
            file_id = file['id']
            # Store file id for metadata retrieval
            file_ids.append(file_id)
            file_content = client.get_file_content(file_id).decode('utf-8')

            try:
                json_data = parse_function(file_content)
            except Exception as e:
                raise ValueError(f"Error parsing file {file_id}: {e}")

            # Adding file_id to each dictionary in the list
            for row in json_data:
                row["file_id"] = file_id

            yield json_data

    # Yield a resource for each folder
    for folder in folders:
        folder_id = folder['folder_id']
        table_name = folder['table_name']
        mime_type = folder['mime_type']
        primary_key = folder.get('primary_key')

        yield dlt.resource(
            get_files(drive_id, folder_id, mime_type),
            table_name=table_name,
            write_disposition="append",
            primary_key=primary_key
        )

    @dlt.resource(
        table_name="file_metadata",
        write_disposition="merge",
        primary_key="id")
    def get_file_metadata():
        for file_id in file_ids:
            yield client.get_file_metadata(file_id)

    yield get_file_metadata
