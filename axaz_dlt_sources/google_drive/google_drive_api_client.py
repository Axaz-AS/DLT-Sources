from typing import Iterable, Sequence, Union, List, Dict

import csv
import io
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import openpyxl  # Added for xlsx parsing

from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials


class GoogleDriveClient:

    def __init__(self, credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials]):
        if isinstance(credentials, GcpOAuthCredentials):
            credentials.auth(
                "https://www.googleapis.com/auth/drive"
            )

        self.service = build(
            "drive",
            "v3",
            credentials=credentials.to_native_credentials()
        )

    def list_files_in_folder(
            self,
            drive_id: str,
            folder_id: str,
            mime_type: str = None,
            modified_since: datetime = None
    ):
        """ Search file in drive location
        """
        query = f"'{folder_id}' in parents"
        if modified_since:
            query += f" and modifiedTime > '{modified_since.isoformat()}'"
        if mime_type:
            query += f" and mimeType = '{mime_type}'"

        try:
            files = []
            page_token = None
            while True:
                response = (
                    self.service.files()
                    .list(
                        q=query,
                        corpora="drive",
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        driveId=drive_id,
                        fields="nextPageToken, files(id, name, mimeType)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                files.extend(response.get("files", []))
                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break

        except HttpError as error:
            print(f"An error occurred: {error}")
            files = None

        return files

    def get_file_metadata(self, file_id: str):
        fields = "id, name, fileExtension, mimeType, size, createdTime, modifiedTime, driveId, parents"
        response = (
            self.service.files()
            .get(fileId=file_id,
                 supportsAllDrives=True,
                 fields=fields
                 )
            .execute()
        )
        return response

    def get_file_content(self, file_id: str):
        response = (
            self.service.files()
            .get_media(fileId=file_id)
            .execute()
        )
        return response

    def parse_csv_to_json(self, csv_content: str):
        csv_file = io.StringIO(csv_content.decode('utf-8'))
        reader = csv.DictReader(csv_file)
        json_data = [row for row in reader]
        return json_data

    def parse_xlsx_to_json(self, xlsx_content: bytes):
        workbook = openpyxl.load_workbook(io.BytesIO(xlsx_content))
        sheet = workbook.active
        data = []

        # Get the headers
        headers = [cell.value for cell in sheet[1]]

        # Iterate through the rows
        for row in sheet.iter_rows(min_row=2, values_only=True):
            data.append(dict(zip(headers, row)))

        return data


if __name__ == "__main__":

    # Load the credentials from the .credentials.json file
    with open('.credentials.json', 'r') as file:
        credentials_json = json.load(file)

    credentials_json_str = json.dumps(credentials_json)
    service_account_credentials = GcpServiceAccountCredentials()
    service_account_credentials.parse_native_representation(
        credentials_json_str)
    client = GoogleDriveClient(credentials=service_account_credentials)

    drive_id = "0AFbBVWlZzGRTUk9PVA"
    folder_id = "1crTGHjoeTgFwCqOKt6BguAyXfmlIbqXj"
    # modified_since_str = "2024-06-10T11:31:00.000Z"

    files = client.list_files_in_folder(
        drive_id, folder_id,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    for file in files:
        file_id = file['id']
        file_content = client.get_file_content(file_id)
        parsed_data = client.parse_xlsx_to_json(file_content)
        print(f"Parsed data for file {file['name']}:")
        print(parsed_data)
