"""
Cherre GraphQL Extraction
=========================
Functions for extracting data from Cherre's GraphQL API.
"""

import json
import time
import requests
from collections import defaultdict
from typing import Any, Dict, List, Optional
from prefect import task, get_run_logger

from src.config import get_settings


def query_cherre(query: str, variables: dict = None, retry: int = 0) -> Optional[dict]:
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
        "Content-Type": "application/json"
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    
    try:
        response = requests.post(
            settings.cherre_api_url, 
            headers=headers, 
            json=payload, 
            timeout=60
        )
        
        if response.status_code != 200:
            print(f"⚠️  HTTP {response.status_code}: {response.text[:200]}")
            if retry < max_retries:
                wait_time = 5 if response.status_code == 500 else 1
                print(f"   Waiting {wait_time}s before retry ({retry + 1}/{max_retries})...")
                time.sleep(wait_time)
                return query_cherre(query, variables, retry + 1)
            return None
        
        result = response.json()
        
        if 'errors' in result:
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
    page_size: int = None,
    max_records: int = None
) -> List[dict]:
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
        
        if not result or 'data' not in result or table_name not in result['data']:
            break
        
        records = result['data'][table_name]
        
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


# Transaction fields for GraphQL query
TRANSACTION_FIELDS = """
    recorder_id
    tax_assessor_id
    document_recorded_date
    document_instrument_date
    document_number_formatted
    document_type_code
    document_amount
    transfer_tax_amount
    arms_length_code
    inter_family_flag
    is_foreclosure_auction_sale
    is_quit_claim
    new_construction_flag
    resale_flag
    property_address
    property_city
    property_state
    property_zip
    cherre_ingest_datetime
    recorder_grantor_v2__recorder_id {
        cherre_recorder_grantor_pk
        grantor_name
        grantor_address
        grantor_entity_code
        grantor_first_name
        grantor_last_name
    }
    recorder_grantee_v2__recorder_id {
        cherre_recorder_grantee_pk
        grantee_name
        grantee_address
        grantee_entity_code
        grantee_first_name
        grantee_last_name
    }
"""


@task(name="extract-transactions", retries=2, retry_delay_seconds=30)
def extract_transactions(start_date: str, end_date: str) -> List[dict]:
    """
    Extract transactions from Cherre for a date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        List of transaction records with nested grantors/grantees
    """
    logger = get_run_logger()
    logger.info(f"📊 Extracting transactions from {start_date} to {end_date}")
    
    transactions = paginated_query(
        table_name="recorder_v2",
        fields=TRANSACTION_FIELDS,
        where_clause=f'where: {{document_recorded_date: {{_gte: "{start_date}", _lte: "{end_date}"}}}}',
        order_by='order_by: {document_recorded_date: asc}'
    )
    
    logger.info(f"✅ Extracted {len(transactions):,} transactions")
    return transactions


@task(name="extract-properties", retries=2, retry_delay_seconds=30)
def extract_properties(transactions: List[dict]) -> List[dict]:
    """
    Extract multifamily property data for transactions.
    
    Args:
        transactions: List of transaction records
        
    Returns:
        List of property records (filtered to multifamily only)
    """
    logger = get_run_logger()
    settings = get_settings()
    
    # Get unique tax_assessor_ids from transactions
    unique_tax_ids = set()
    for txn in transactions:
        if txn.get('tax_assessor_id'):
            unique_tax_ids.add(txn['tax_assessor_id'])
    
    logger.info(f"📊 Found {len(unique_tax_ids):,} unique properties in transactions")
    
    # Query properties in batches (MULTIFAMILY ONLY)
    properties = []
    tax_id_list = list(unique_tax_ids)
    batch_size = settings.batch_size
    mf_codes = settings.mf_codes
    
    for i in range(0, len(tax_id_list), batch_size):
        batch = tax_id_list[i:i+batch_size]
        
        query = f"""
        query {{
            tax_assessor_v2(
                where: {{
                    tax_assessor_id: {{_in: {json.dumps(batch)}}}
                    property_use_standardized_code: {{_in: {json.dumps(mf_codes)}}}
                }}
            ) {{
                tax_assessor_id
                assessor_parcel_number_raw
                address
                city
                state
                zip
                situs_county
                property_use_standardized_code
                year_built
                building_sq_ft
                lot_size_sq_ft
                units_count
                assessed_value_total
                market_value_total
                latitude
                longitude
            }}
        }}
        """
        
        result = query_cherre(query)
        if result and 'data' in result:
            properties.extend(result['data']['tax_assessor_v2'])
        
        if (i + batch_size) % 1000 == 0:
            logger.info(f"   Processed {i + batch_size:,} property IDs...")
    
    logger.info(f"✅ Extracted {len(properties):,} MULTIFAMILY properties")
    return properties


@task(name="extract-property-history", retries=2, retry_delay_seconds=30)
def extract_property_history(properties: List[dict]) -> List[dict]:
    """
    Extract property history for SCD Type 2.
    
    Args:
        properties: List of property records
        
    Returns:
        List of property history records
    """
    logger = get_run_logger()
    settings = get_settings()
    
    # Only fetch history for MF properties
    tax_id_list = list(set(p['tax_assessor_id'] for p in properties))
    logger.info(f"📊 Fetching history for {len(tax_id_list):,} properties")
    
    property_history = []
    batch_size = settings.batch_size
    
    for i in range(0, len(tax_id_list), batch_size):
        batch = tax_id_list[i:i+batch_size]
        
        query = f"""
        query {{
            tax_assessor_history_v2(
                where: {{tax_assessor_id: {{_in: {json.dumps(batch)}}}}}
                order_by: {{cherre_tax_assessor_history_v2_pk: asc}}
            ) {{
                tax_assessor_id
                assessor_snap_shot_year
                assessed_value_total
                market_value_total
                lot_size_sq_ft
                building_sq_ft
                cherre_tax_assessor_history_v2_pk
                cherre_ingest_datetime
            }}
        }}
        """
        
        result = query_cherre(query)
        if result and 'data' in result:
            property_history.extend(result['data']['tax_assessor_history_v2'])
    
    logger.info(f"✅ Extracted {len(property_history):,} property history records")
    return property_history


@task(name="extract-entities", retries=2, retry_delay_seconds=30)
def extract_entities(properties: List[dict]) -> List[dict]:
    """
    Extract entity/owner data for properties.
    
    Args:
        properties: List of property records
        
    Returns:
        List of entity records from usa_owner_unmask_v2
    """
    logger = get_run_logger()
    settings = get_settings()
    
    tax_id_list = list(set(p['tax_assessor_id'] for p in properties))
    logger.info(f"📊 Fetching entities for {len(tax_id_list):,} properties")
    
    entities = []
    batch_size = settings.batch_size
    
    for i in range(0, len(tax_id_list), batch_size):
        batch = tax_id_list[i:i+batch_size]
        
        query = f"""
        query {{
            usa_owner_unmask_v2(where: {{tax_assessor_id: {{_in: {json.dumps(batch)}}}}}) {{
                cherre_usa_owner_unmask_pk
                owner_id
                owner_name
                owner_type
                owner_state
                has_confidence
                occurrences_count
                last_seen_date
                tax_assessor_id
            }}
        }}
        """
        
        result = query_cherre(query)
        if result and 'data' in result:
            entities.extend(result['data']['usa_owner_unmask_v2'])
    
    logger.info(f"✅ Extracted {len(entities):,} entity records")
    return entities

