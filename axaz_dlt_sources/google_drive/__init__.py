from typing import Union, List, Dict, Generator

import dlt
from dlt.common import logger
from dlt.sources import DltResource
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials

from .google_drive_api_client import GoogleDriveClient
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


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
        incremental_cursor=dlt.sources.incremental(
            "row_inserted_at", initial_value="1970-01-01T00:00:00Z"
        )
    ):
        """
        Generator that fetches files from a specific folder.
        dlt will automatically update the incremental state by finding the
        maximum value of the 'row_inserted_at' column in the yielded data.
        """
        last_modified_str = incremental_cursor.last_value
        logger.info(f"Files last modified since: {last_modified_str}")

        # Convert string from state to a datetime object for the API call
        modified_since = datetime.fromisoformat(last_modified_str.replace("Z", "+00:00"))

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
            file_modified_time_str = file['row_inserted_at']
            file_id = file['id']

            file_ids.append(file_id)
            file_content = client.get_file_content(file_id)

            try:
                json_data = parse_function(file_content)
            except Exception as e:
                raise ValueError(f"Error parsing file {file_id}: {e}")

            # Add the cursor column to each row. dlt will use this to update the state.
            for row in json_data:
                row["file_id"] = file_id
                row["row_inserted_at"] = file_modified_time_str

            yield json_data

    for folder in folders:
        folder_id = folder['folder_id']
        table_name = folder['table_name']
        mime_type = folder['mime_type']
        primary_key = folder.get('primary_key') 

        print(folder)

        yield dlt.resource(
            get_files,
            name=table_name,
            write_disposition="replace",
            primary_key=primary_key
        )(drive_id, folder_id, mime_type)

    @dlt.resource(
        name="file_metadata",
        write_disposition="merge",
        primary_key="id")
    def get_file_metadata():
        files = []
        for file_id in file_ids:
            files.append(client.get_file_metadata(file_id))
        yield files

    yield get_file_metadata
