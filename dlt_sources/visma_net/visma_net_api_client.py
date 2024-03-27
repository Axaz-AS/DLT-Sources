
from dlt.sources.helpers import requests
import time
import math
from typing import List
from datetime import datetime, timedelta

from typing import Any, Dict, Iterator, List, Optional


class VismaNetClient:
    # Global dictionary to store tokens
    tokens = {}
    # Credentials
    client_id: str
    client_secret: str
    tenant_ids: List[str]
    # Base URL
    base_url = "https://integration.visma.net/API"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_ids: List[str]
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_ids = tenant_ids

    def get_access_token(self, tenant_id: str) -> str:
        current_time = time.time()

        if tenant_id in self.tokens:
            token_info = self.tokens[tenant_id]
            if current_time < token_info['expires_time']:
                return token_info['access_token']

        token_url = 'https://connect.visma.com/connect/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'tenant_id': tenant_id
        }

        response = requests.post(token_url, data=data)
        response.raise_for_status()
        token_response = response.json()

        expires_time = current_time + token_response['expires_in']
        self.tokens[tenant_id] = {
            'access_token': token_response['access_token'],
            'expires_time': expires_time
        }

        return token_response['access_token']

    def api_get_request(self, tenant_id, path, query_params=None):
        access_token = self.get_access_token(tenant_id)
        headers = {'Authorization': f'Bearer {access_token}'}

        url = f'{self.base_url}{path}'

        response = requests.get(url, params=query_params, headers=headers)
        response.raise_for_status()
        response_json = response.json()

        for item in response_json:
            item['tenant_id'] = tenant_id
        return response_json

    def get_data_from_endpoint(
        self,
        path: str,
        last_modified: Optional[str] = None
    ) -> Iterator[List[Dict[str, Any]]]:

        for tenant_id in self.tenant_ids:

            query_params = {}
            if last_modified:
                query_params['lastModifiedDateTime'] = last_modified
                query_params['lastModifiedDateTimeCondition'] = '>'

            response_json = self.api_get_request(
                tenant_id=tenant_id,
                path=path,
                query_params=query_params
            )
            yield response_json

    def get_paginated_data_from_endpoint(
        self,
        path: str,
        page_size: int = 1000,
        page_size_param: str = 'pageSize',
        page_nr_param: str = 'pageNumber',
        last_modified: Optional[str] = None,
        other_query_params: Optional[Dict[str, Any]] = None
    ) -> Iterator[List[Dict[str, Any]]]:

        for tenant_id in self.tenant_ids:
            query_params: Dict[str, Any] = {
                page_size_param: page_size,
                page_nr_param: 1
            }
            if last_modified:
                query_params['lastModifiedDateTime'] = last_modified
                query_params['lastModifiedDateTimeCondition'] = '>'
            if other_query_params:
                query_params = dict(query_params, **other_query_params)

            while True:
                response_json = self.api_get_request(
                    tenant_id=tenant_id,
                    path=path,
                    query_params=query_params
                )

                row_count = len(response_json)
                if row_count > 0:
                    yield response_json

                if row_count < page_size:
                    break
                else:
                    query_params[page_nr_param] += 1

    def prev_period(self, period: str) -> str:
        """Return the previous financial period in the format YYYYMM.
        """
        period_date = datetime.strptime(period, "%Y%m")
        # Subtract one month
        first_day_of_current_period = period_date.replace(day=1)
        last_day_of_prev_period = first_day_of_current_period - timedelta(days=1)
        return last_day_of_prev_period.strftime("%Y%m")

    def get_journal_transactions_by_period(
        self,
        page_size: int,
        from_period: str
    ) -> Iterator[List[Dict[str, Any]]]:

        current_period = datetime.now().strftime("%Y%m")
        while current_period >= from_period:
            n_pages = 0
            for page in self.get_paginated_data_from_endpoint(
                path='/controller/api/v2/journaltransaction',
                page_size=page_size,
                other_query_params={
                    'periodId': current_period
                }
            ):
                yield page
                n_pages += 1

            if n_pages == 0:
                break

            current_period = self.prev_period(current_period)
