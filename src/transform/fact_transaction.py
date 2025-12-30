"""
Transaction Fact Builder
========================
Builds fact_transaction from Cherre transaction data.
"""

from typing import Dict, List, Tuple
from prefect import task, get_run_logger


@task(name="build-fact-transaction")
def build_fact_transaction(
    transactions_raw: List[dict], 
    property_key_lookup: Dict
) -> Tuple[List[dict], Dict]:
    """
    Build fact_transaction from raw transaction data.
    
    Args:
        transactions_raw: List of transaction records from recorder_v2
        property_key_lookup: Dict mapping tax_assessor_id → property_key
        
    Returns:
        Tuple of (fact_transaction records, transaction_key_lookup dict)
    """
    logger = get_run_logger()
    logger.info("🔨 Building fact_transaction...")
    
    fact_transaction = []
    transaction_key_counter = 1
    
    for txn in transactions_raw:
        # Classify transaction
        is_sale = False
        transaction_category = 'OTHER'
        
        if txn.get('arms_length_code') and txn.get('document_amount') and txn['document_amount'] > 0:
            is_sale = True
            transaction_category = 'SALE'
        elif txn.get('document_amount') == 0:
            transaction_category = 'MORTGAGE'
        
        # Get property_key
        property_key = None
        if txn.get('tax_assessor_id'):
            property_key = property_key_lookup.get(txn['tax_assessor_id'])
        
        # Count parties
        grantor_count = len(txn.get('recorder_grantor_v2__recorder_id', []))
        grantee_count = len(txn.get('recorder_grantee_v2__recorder_id', []))
        
        fact_transaction.append({
            'transaction_key': transaction_key_counter,
            'recorder_id': txn['recorder_id'],
            'property_key': property_key,
            'transaction_date': txn.get('document_recorded_date'),
            'instrument_date': txn.get('document_instrument_date'),
            'document_number': txn.get('document_number_formatted'),
            'document_type_code': txn.get('document_type_code'),
            'document_amount': txn.get('document_amount'),
            'transfer_tax_amount': txn.get('transfer_tax_amount'),
            'arms_length_flag': txn.get('arms_length_code'),
            'inter_family_flag': txn.get('inter_family_flag'),
            'is_foreclosure': txn.get('is_foreclosure_auction_sale'),
            'is_quit_claim': txn.get('is_quit_claim'),
            'new_construction_flag': txn.get('new_construction_flag'),
            'resale_flag': txn.get('resale_flag'),
            'transaction_category': transaction_category,
            'is_sale': is_sale,
            'property_address': txn.get('property_address'),
            'property_city': txn.get('property_city'),
            'property_state': txn.get('property_state'),
            'property_zip': txn.get('property_zip'),
            'tax_assessor_id': txn.get('tax_assessor_id'),
            'grantor_count': grantor_count,
            'grantee_count': grantee_count,
            'has_multiple_parties': (grantor_count + grantee_count) > 2,
            'source_system': 'cherre',
            'cherre_ingest_datetime': txn.get('cherre_ingest_datetime')
        })
        
        transaction_key_counter += 1
    
    # Create lookup: recorder_id → transaction_key
    transaction_key_lookup = {t['recorder_id']: t['transaction_key'] for t in fact_transaction}
    
    # Stats
    sales_count = sum(1 for t in fact_transaction if t['is_sale'])
    with_property = sum(1 for t in fact_transaction if t['property_key'])
    
    logger.info(f"✅ Created {len(fact_transaction):,} fact_transaction records")
    logger.info(f"   Sales: {sales_count:,} | With property_key: {with_property:,}")
    
    return fact_transaction, transaction_key_lookup

