import dlt
from dlt.sources import DltResource
from typing import List, Sequence
from .visma_net_api_client import VismaNetClient
from datetime import datetime, timedelta


@dlt.source(name='visma_net')
def visma_net(
    tenant_ids: List[str]=dlt.config.value,
    client_id: str=dlt.secrets.value,
    client_secret: str=dlt.secrets.value
) -> Sequence[DltResource]:
    """ Source for Visma NET ERP Service API
    """

    client = VismaNetClient(
        client_id = client_id,
        client_secret = client_secret,
        tenant_ids = tenant_ids
    )

    @dlt.resource(
        table_name="account",
        write_disposition="append",
        primary_key="accountID")
    def get_account(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        yield from client.get_data_from_endpoint(
            path='/controller/api/v1/account',
            last_modified=last_modified
        )


    @dlt.resource(
        table_name="contact",
        write_disposition="append",
        primary_key="contactId")
    def get_contact(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        yield from client.get_data_from_endpoint(
            path='/controller/api/v1/contact',
            last_modified=last_modified
        )


    @dlt.resource(
        table_name="inventory",
        write_disposition="append",
        primary_key="inventoryId")
    def get_inventory(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        yield from client.get_paginated_data_from_endpoint(
            path='/controller/api/v1/inventory',
            page_size=5000,
            page_size_param='pageSize',
            page_nr_param='pageNumber',
            last_modified=last_modified
        )


    @dlt.resource(
        table_name="customer",
        write_disposition="append",
        primary_key="internalId")
    def get_customer(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        yield from client.get_paginated_data_from_endpoint(
            path='/controller/api/v1/customer',
            page_size=1000,
            page_size_param='pageSize',
            page_nr_param='pageNumber',
            last_modified=last_modified
        )


    @dlt.resource(
        table_name="supplier",
        write_disposition="append",
        primary_key="internalId")
    def get_supplier(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        yield from client.get_paginated_data_from_endpoint(
            path='/controller/api/v1/supplier',
            page_size=1000,
            page_size_param='pageSize',
            page_nr_param='pageNumber',
            last_modified=last_modified
        )


    @dlt.resource(
        table_name="subaccount",
        write_disposition="append",
        primary_key="subaccountId")
    def get_subaccount(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        yield from client.get_paginated_data_from_endpoint(
            path='/controller/api/v1/subaccount',
            page_size=1000,
            last_modified=last_modified
        )


    @dlt.resource(
        table_name="general_ledger_transactions",
        write_disposition="merge",
        primary_key="batchNumber",
        merge_key="batchNumber")
    def get_general_ledger_transactions(
        updated_at = dlt.sources.incremental("lastModifiedDateTime", initial_value="1970-01-01T00:00:00Z")
    ):
        dt = datetime.fromisoformat(updated_at.start_value.rstrip("Z"))
        last_modified = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        six_months_ago = datetime.now() - timedelta(days=6*30)
        if dt > six_months_ago:
            yield from client.get_paginated_data_from_endpoint(
                    path='/controller/api/v2/journaltransaction',
                    page_size=1000,
                    last_modified=last_modified
                )
        else:
            yield from client.get_journal_transactions_by_period(
                page_size=1000,
                from_period=dt.strftime("%Y%m")
            )



    return [
        get_account,
        get_contact,
        get_inventory,
        get_customer,
        get_supplier,
        get_subaccount,
        get_general_ledger_transactions
    ]
