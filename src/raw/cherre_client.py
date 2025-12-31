"""
Cherre GraphQL Client
=====================
Shared client for Cherre API calls used by raw modules.
"""

import time

import requests

from src.config import get_settings


def query_cherre(query: str, variables: dict | None = None, retry: int = 0) -> dict | None:
    """
    Execute GraphQL query against Cherre API with retry logic.

    Args:
        query: GraphQL query string
        variables: Optional query variables
        retry: Current retry count (internal use)

    Returns:
        Query result dict or None on failure
    """
    settings = get_settings()
    max_retries = settings.max_retries

    headers = {
        "Authorization": f"Bearer {settings.cherre_api_key}",
        "Content-Type": "application/json",
    }
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables

    try:
        response = requests.post(settings.cherre_api_url, headers=headers, json=payload, timeout=60)

        if response.status_code != 200:
            print(f"⚠️  HTTP {response.status_code}: {response.text[:200]}")
            if retry < max_retries:
                wait_time = 5 if response.status_code == 500 else 1
                print(f"   Waiting {wait_time}s before retry ({retry + 1}/{max_retries})...")
                time.sleep(wait_time)
                return query_cherre(query, variables, retry + 1)
            return None

        result = response.json()

        if "errors" in result:
            print(f"⚠️  GraphQL Error: {result['errors']}")
            return None

        return result

    except Exception as e:
        print(f"⚠️  Request failed: {e}")
        if retry < max_retries:
            print(f"   Retrying ({retry + 1}/{max_retries})...")
            time.sleep(1)
            return query_cherre(query, variables, retry + 1)
        return None


def paginated_query(
    table_name: str,
    fields: str,
    where_clause: str = "",
    order_by: str = "",
    page_size: int | None = None,
    max_records: int | None = None,
) -> list[dict]:
    """
    Fetch data from Cherre with pagination.

    Args:
        table_name: GraphQL table name (e.g., 'recorder_v2')
        fields: GraphQL fields to fetch
        where_clause: Optional where filter
        order_by: Optional ordering
        page_size: Records per page (defaults to settings.page_size)
        max_records: Max total records to fetch (None = unlimited)

    Returns:
        List of records
    """
    settings = get_settings()
    if page_size is None:
        page_size = settings.page_size

    all_records = []
    offset = 0

    print(f"\n📊 Querying {table_name}...")

    while True:
        query = f"""
        query {{
            {table_name}(
                limit: {page_size}
                offset: {offset}
                {where_clause}
                {order_by}
            ) {{
                {fields}
            }}
        }}
        """

        result = query_cherre(query)

        if not result or "data" not in result or table_name not in result["data"]:
            break

        records = result["data"][table_name]

        if not records:
            break

        all_records.extend(records)
        offset += page_size
        print(f"   Fetched {len(all_records):,} records so far...")

        if max_records and len(all_records) >= max_records:
            all_records = all_records[:max_records]
            break

    print(f"   ✅ Fetched {len(all_records):,} total records")
    return all_records
