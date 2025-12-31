# NEDL ETL Migration

This document tracks the migration from the legacy `cre-dashboard-cherre-refresh-scripts` (Jupyter notebooks) to the new `nedl-data` (Prefect + Supabase) system.

**Source Repo:** `cre-dashboard-cherre-refresh-scripts/`  
**Target Repo:** `nedl-data/`

---

## Raw Layer

Raw layer extracts data from source systems and lands it in the `raw` schema as append-only tables.

---

### 1. Cherre Transactions

Recorder/deed transaction records from Cherre GraphQL API.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: GraphQL query `recorder_v2__tax_assessor_id` with 40+ fields
- cell[22]: `recorder_df = pd.concat(rec_table_list)` creates combined DataFrame

**Output Fields:**
- `recorder_id`, `tax_assessor_id`, `document_recording_state`, `document_recording_county`
- `document_type_code`, `document_number_formatted`, `document_amount`, `arms_length_code`
- `property_address`, `property_city`, `property_state`, `property_zip`
- `document_recorded_date`, `transfer_tax_amount`, `resale_flag`, `new_construction_flag`
- `inter_family_flag`, `reo_sale_flag`, `cherre_ingest_datetime`

**Status:** DONE

**Implementation:**
- [x] `src/raw/cherre_transactions.py` - extracts from `recorder_v2`
- [x] `src/raw/cherre_client.py` - shared GraphQL client
- [x] Supabase table `raw.cherre_transactions`
- [x] Integrated into `src/flows/extract.py`

---

### 2. Cherre Grantors

Grantor (seller) party data from recorder transactions.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: Nested `recorder_grantor_v2__recorder_id` in GraphQL query
- cell[30]: Explode and normalize: `normalized_df_grantor = pd.json_normalize(exploded_df_grantor['recorder_grantor_v2__recorder_id'])`

**Output Fields:**
- `cherre_recorder_grantor_pk`, `recorder_id`, `grantor_name`, `grantor_address`
- `grantor_city`, `grantor_state`, `grantor_zip`, `grantor_entity_code`
- `grantor_first_name`, `grantor_last_name`, `grantor_middle_name`
- `cherre_ingest_datetime`

**Status:** DONE

**Implementation:**
- [x] `src/raw/cherre_grantors.py` - extracts from nested `recorder_grantor_v2`
- [x] Supabase table `raw.cherre_grantors`
- [x] Integrated into `src/flows/extract.py`

---

### 3. Cherre Grantees

Grantee (buyer) party data from recorder transactions.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: Nested `recorder_grantee_v2__recorder_id` in GraphQL query
- cell[29]: Explode and normalize: `normalized_df_grantee = pd.json_normalize(exploded_df_grantee['recorder_grantee_v2__recorder_id'])`

**Output Fields:**
- `cherre_recorder_grantee_pk`, `recorder_id`, `grantee_name`, `grantee_address`
- `grantee_city`, `grantee_state`, `grantee_zip`, `grantee_entity_code`
- `grantee_first_name`, `grantee_last_name`, `grantee_middle_name`
- `grantee_owner_type`, `grantee_vesting_deed_code`
- `cherre_ingest_datetime`

**Status:** DONE

**Implementation:**
- [x] `src/raw/cherre_grantees.py` - extracts from nested `recorder_grantee_v2`
- [x] Supabase table `raw.cherre_grantees`
- [x] Integrated into `src/flows/extract.py`

---

### 4. Cherre Properties (Tax Assessor)

Property/parcel data from Cherre tax assessor records.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: GraphQL query `tax_assessor_v2__tax_assessor_id` with 60+ fields
- cell[22]: `tax_assessor_id_df = pd.concat(tax_id_table_list)`
- cell[39]: Drop nested columns before storage

**Output Fields:**
- `tax_assessor_id`, `situs_state`, `situs_county`, `fips_code`
- `assessor_parcel_number_raw`, `address`, `city`, `state`, `zip`
- `latitude`, `longitude`, `year_built`, `building_sq_ft`, `units_count`
- `assessed_value_total`, `market_value_total`, `property_use_standardized_code`
- `last_sale_amount`, `last_sale_date`, `cherre_ingest_datetime`

**Status:** DONE

**Implementation:**
- [x] `src/raw/cherre_properties.py` - extracts from `tax_assessor_v2`
- [x] Supabase table `raw.cherre_properties`
- [x] Integrated into `src/flows/extract.py`

---

### 5. Cherre Mortgages

Mortgage/loan records from recorder transactions.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: Nested `recorder_mortgage_v2__recorder_id` in GraphQL query (lines containing mortgage fields)
- cell[31]: Explode and normalize: `normalized_df_mortgage = pd.json_normalize(exploded_df_mortgage['recorder_mortgage_v2__recorder_id'])`
- cell[33]: Filter null rows

**Output Fields:**
- `cherre_recorder_mortgage_pk`, `recorder_id`, `document_number`
- `document_recorded_date`, `type_code`, `amount`, `lender_name`
- `term`, `term_type_code`, `mortgage_due_date`
- `interest_rate`, `interest_rate_type_code`, `is_adjustable_rate_rider`
- `has_pre_payment_penalty`, `has_interest_only_period`
- `cherre_ingest_datetime`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/raw/cherre_mortgages.py`
  - Extract from nested `recorder_mortgage_v2__recorder_id` in transaction response
  - Follow pattern in `src/raw/cherre_grantors.py:34-55`
- [ ] Add `cherre_mortgages` to `src/raw/__init__.py` exports
- [ ] Create Supabase table DDL for `raw.cherre_mortgages`
- [ ] Add sync call in `src/flows/extract.py` after transactions sync

---

### 6. Cherre Tax Assessor Owners

Property owner data from tax assessor records.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: Nested `tax_assessor_owner_v2__tax_assessor_id` in GraphQL query
- cell[27]: Explode and normalize: `normalized_df_owner = pd.json_normalize(exploded_df_owner['tax_assessor_owner_v2__tax_assessor_id'])`
- cell[37]: Filter null rows

**Output Fields:**
- `cherre_tax_assessor_owner_pk`, `tax_assessor_id`
- `owner_name`, `owner_first_name`, `owner_last_name`, `owner_middle_name`
- `owner_name_suffix`, `owner_type`, `is_owner_company`
- `owner_trust_type_code`, `ownership_vesting_relation_code`
- `cherre_ingest_datetime`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/raw/cherre_tax_assessor_owners.py`
  - Extract from nested `tax_assessor_owner_v2__tax_assessor_id` in property response
  - Follow pattern in `src/raw/cherre_grantors.py`
- [ ] Add to `src/raw/__init__.py` exports
- [ ] Create Supabase table DDL for `raw.cherre_tax_assessor_owners`
- [ ] Add sync call in `src/flows/extract.py` after properties sync

---

### 7. Cherre Tax Assessor Owners Unmask

Unmasked/enriched owner data with confidence scores.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[15]: Nested `usa_owner_unmask_v2__tax_assessor_id` in GraphQL query
- cell[28]: Explode and normalize: `normalized_df_unmask = pd.json_normalize(exploded_df_usa_unmask['usa_owner_unmask_v2__tax_assessor_id'])`
- cell[36]: Filter null rows

**Output Fields:**
- `cherre_usa_owner_unmask_pk`, `tax_assessor_id`, `owner_id`
- `owner_name`, `owner_state`, `owner_type`
- `has_confidence`, `occurrences_count`, `last_seen_date`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/raw/cherre_owners_unmask.py`
  - Extract from nested `usa_owner_unmask_v2__tax_assessor_id` in property response
  - Follow pattern in `src/raw/cherre_grantors.py`
- [ ] Add to `src/raw/__init__.py` exports
- [ ] Create Supabase table DDL for `raw.cherre_owners_unmask`
- [ ] Add sync call in `src/flows/extract.py` after properties sync

---

### 8. Cherre Demographics

Census demographic data by geography (census tract).

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/06_raw_demographic_v2_10_01_24_prod.ipynb`
- cell[5]: GraphQL query `usa_demographics_v2__geography_code` with 150+ fields
  - Population counts by age bracket (current + 5-year forecast)
  - Household counts by size and income bracket
  - Employment by occupation category
  - Crime risk indices
  - Labor force statistics

**Output Fields:** (150+ fields including)
- `geography_code`, `geography_name`, `geography_type`, `vintage`
- `total_population_count`, `population_5_year_forecast`, `population_median_age`
- `household_count`, `household_average_size`, `median_household_income`
- `labor_force_unemployment_rate`, `crime_total_risk`
- Population age brackets: `population_age_00_04_count` through `population_age_over_85_count`
- Household income brackets: `household_income_0_10_count` through `household_income_over_500_count`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/raw/cherre_demographics.py`
  - GraphQL query via `usa_census_tract_boundary__geo_id_pk` → `usa_demographics_v2__geography_code`
  - Filter by `vintage: "2024A"` (or latest)
  - Requires `tax_assessor_id` and `zip` to traverse relationship
- [ ] Add to `src/raw/__init__.py` exports
- [ ] Create Supabase table DDL for `raw.cherre_demographics`
- [ ] Add sync call in `src/flows/extract.py` - run after properties are synced

---

### 9. Yardi Properties

Property master data from Yardi flat files (Google Drive).

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/05_Incremental_src_enrich_script_prod.ipynb`
- cell[9]: Query from `yardi_src.yardi_all_properties_*` tables (monthly snapshots)
- cell[11]: Merge monthly files with priority to latest
- cell[13]: Select columns for `yardi_filter`

**Output Fields:**
- `property_id`, `property_name`, `address`, `city`, `county`, `state`, `zip`
- `units`, `sqft`, `completion_date`, `latitude`, `longitude`
- `latest_sale_date`, `latest_sale_price($)`
- `owner`, `owner_contact_first_name`, `owner_contact_last_name`
- `owner_contact_email`, `owner_contact_phone_number`
- `owner_address`, `owner_city`, `owner_state`, `owner_zip`, `owner_country`

**Status:** TODO

**Subtasks:**
- [ ] Create `scripts/load_yardi_properties.py`
  - Read CSV/Excel from local path (downloaded from Google Drive)
  - Upsert to Supabase `raw.yardi_properties`
- [ ] Create Supabase table DDL for `raw.yardi_properties`
- [ ] Document Google Drive folder structure and file naming convention
- [ ] Add Makefile target: `make load-yardi-properties FILE=path/to/file.csv`

---

### 10. Yardi Loans

Loan/mortgage data from Yardi flat files.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/05_Incremental_src_enrich_script_prod.ipynb`
- cell[21]: Query from `yardi_src.yardi_loans_*` tables with `loan_status='Current'`
- cell[25]: Convert `loan_amount(MM)` to full dollars: `* 1_000_000`
- cell[26-28]: Deduplicate by `property_name`, `zip`, `loan_origination_date`, `loan_maturity_date`

**Output Fields:**
- `property_id`, `property_name`, `zip`
- `loan_amount(MM)` (converted to full dollars)
- `loan_origination_date`, `loan_maturity_date`, `loan_status`
- `lender` (renamed to `lender_name`), `originator` (renamed to `originator_name`)

**Status:** TODO

**Subtasks:**
- [ ] Create `scripts/load_yardi_loans.py`
  - Read CSV/Excel from local path
  - Filter to `loan_status='Current'`
  - Convert `loan_amount(MM)` to dollars
  - Deduplicate logic per cell[26-28]
  - Upsert to Supabase `raw.yardi_loans`
- [ ] Create Supabase table DDL for `raw.yardi_loans`
- [ ] Add Makefile target: `make load-yardi-loans FILE=path/to/file.csv`

---

### 11. Yardi Sales

Property sales transaction data from Yardi flat files.

**Source:** `cre-dashboard-cherre-refresh-scripts/02_UAT/Refresh/UAT_yardi_enrich_insertion_23_10_24.ipynb`
- cell[5208 area]: Query from `yardi_src.yardi_property_sales_*` tables

**Output Fields:**
- `property_id`, `property_name`, `zip`
- `sale_date`, `sale_price`, `buyer`, `seller`

**Status:** TODO

**Subtasks:**
- [ ] Create `scripts/load_yardi_sales.py`
  - Read CSV/Excel from local path
  - Upsert to Supabase `raw.yardi_sales`
- [ ] Create Supabase table DDL for `raw.yardi_sales`
- [ ] Add Makefile target: `make load-yardi-sales FILE=path/to/file.csv`

---

### 12. Yardi Rent/Occupancy

Time-series rent and occupancy data from Yardi flat files.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Refresh/raw_rent_occ_appending_new_extracted_data_prod.ipynb`
- cell[3]: Query from `yardi_src.yardi_rent_occ_data_*` with `series = 'Rent Actual'`
- Series values: `'Rent Actual'`, `'Occupancy Rate'`

**Output Fields:**
- `property_id`, `property_name`, `market_name`, `submarket_name`
- `series` (e.g., 'Rent Actual', 'Occupancy Rate')
- `record_date`, `record_values`

**Status:** TODO

**Subtasks:**
- [ ] Create `scripts/load_yardi_rent_occ.py`
  - Read CSV/Excel from local path
  - Support both 'Rent Actual' and 'Occupancy Rate' series
  - Upsert to Supabase `raw.yardi_rent_occupancy`
- [ ] Create Supabase table DDL for `raw.yardi_rent_occupancy`
- [ ] Add Makefile target: `make load-yardi-rent-occ FILE=path/to/file.csv`

---

### 13. Y-C Mapping (Yardi-Cherre Property Link)

Mapping table linking Yardi `property_id` to Cherre `tax_assessor_id`.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/05_Incremental_src_enrich_script_prod.ipynb`
- cell[3]: Read existing mapping: `SELECT * FROM cherre_src.y_c_mapping WHERE active_flag = 'Y'`
- cell[5]: Select key columns: `['nedl_property_id', 'property_id', 'nedl_property_id_pk', 'tax_assessor_id', 'property_name']`

**Source (creation):** `cre-dashboard-cherre-refresh-scripts/02_UAT/Refresh/UAT_Y_C_mapping_and_all_cherre_tables_updation_after_extraction_29_11_24.ipynb`
- Manual Excel-based matching of Yardi properties to Cherre tax assessor records
- `active_flag` column to soft-delete outdated mappings

**Output Fields:**
- `property_id` (Yardi ID)
- `tax_assessor_id` (Cherre ID)
- `nedl_property_id` (derived: `identifier + " " + zip`)
- `nedl_property_id_pk` (primary key)
- `property_name`
- `active_flag` ('Y' or 'N')
- `insert_date`

**Status:** TODO

**Subtasks:**
- [ ] Create `scripts/load_yc_mapping.py`
  - Read mapping CSV/Excel
  - Validate required columns exist
  - Upsert to Supabase `raw.yc_mapping`
- [ ] Create Supabase table DDL for `raw.yc_mapping`
- [ ] Add Makefile target: `make load-yc-mapping FILE=path/to/file.csv`
- [ ] Document process for updating mappings when new Yardi properties are added

---

## Analytics Layer

Analytics layer transforms raw data into dimensional model for analysis.

---

### 1. dim_property

Property dimension with SCD Type 2 structure.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/03_insertion_transform_tax_assessor_y_c_map_rent_datas_prod.ipynb`
- cells[5-15]: Read `raw_nedl_tax_assessor`, clean dtypes
- cell[17]: Create `nedl_property_id`: `identifier + " " + zip`
- cell[19]: Create `fips_tract_block_group`: concatenate `fips_code + census_tract + census_block_group`

**Output Fields:**
- `property_key`, `tax_assessor_id`, `assessor_parcel_number`
- `property_address`, `property_city`, `property_state`, `property_zip`, `property_county`
- `property_use_code`, `year_built`, `building_sqft`, `land_sqft`, `units_count`
- `assessed_value`, `market_value`, `latitude`, `longitude`
- `valid_from`, `valid_to`, `is_current`, `source_system`

**Status:** DONE

**Implementation:**
- [x] `src/analytics/dim_property.py` - transforms `raw.cherre_properties`
- [x] Supabase table `analytics.dim_property`
- [x] Integrated into `src/flows/transform_analytics.py`

---

### 2. dim_entity

Entity dimension for grantors/grantees with deduplication.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cells[27-30]: Extract owner/grantor/grantee data from nested JSON
- Deduplication by normalized name

**Output Fields:**
- `entity_key`, `canonical_entity_id`, `canonical_entity_name`
- `entity_type` (INDIVIDUAL, CORPORATION, LLC, TRUST, etc.)
- `state`, `confidence_score`, `occurrences_count`
- `is_resolved`, `resolution_method`
- `valid_from`, `valid_to`, `is_current`, `source_system`

**Status:** DONE

**Implementation:**
- [x] `src/analytics/dim_entity.py` - transforms `raw.cherre_grantors` + `raw.cherre_grantees`
- [x] Supabase table `analytics.dim_entity`
- [x] Integrated into `src/flows/transform_analytics.py`

---

### 3. fact_transaction

Transaction fact table linking to dimensions.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/raw_extraction_updated.ipynb`
- cell[22]: `recorder_df` contains all transaction records
- Classification logic for sale vs mortgage

**Output Fields:**
- `transaction_key`, `recorder_id`, `property_key`
- `transaction_date`, `instrument_date`, `document_number`, `document_type_code`
- `document_amount`, `transfer_tax_amount`, `transaction_category`
- `is_sale`, `is_arms_length`, `is_inter_family`, `is_foreclosure`
- `is_quit_claim`, `is_new_construction`, `is_resale`
- `grantor_count`, `grantee_count`
- `property_address`, `property_city`, `property_state`, `property_zip`
- `source_system`, `cherre_ingest_datetime`

**Status:** DONE

**Implementation:**
- [x] `src/analytics/fact_transaction.py` - transforms `raw.cherre_transactions`
- [x] Supabase table `analytics.fact_transaction`
- [x] Integrated into `src/flows/transform_analytics.py`

---

### 4. transform_tax_assessor

Cleaned/typed tax assessor records (intermediate transform).

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/03_insertion_transform_tax_assessor_y_c_map_rent_datas_prod.ipynb`
- cell[12]: dtype conversions - `Int64` for numeric, `date` for datetime columns
- cell[13]: `clean_numeric_columns()` for room/bath/bed/stories/units counts
- cell[14]: `truncate_decimal_in_columns()` for code fields
- cell[17]: Create `nedl_property_id`
- cell[19]: Create `fips_tract_block_group`
- cell[26]: Deduplicate by `tax_assessor_id`
- cell[34]: Insert to `transform_nedl_tax_assessor`

**Output Fields:** 61+ columns from raw tax assessor with:
- Cleaned numeric types
- `nedl_property_id` (derived key)
- `fips_tract_block_group` (derived)
- `insert_date`

**Status:** TODO (partially covered by dim_property)

**Subtasks:**
- [ ] Review if separate `transform_tax_assessor` table is needed or if `dim_property` suffices
- [ ] If needed: Create `src/analytics/transform_tax_assessor.py`
  - Implement dtype cleaning logic from cell[12-14]
  - Implement `nedl_property_id` derivation from cell[17]
  - Implement `fips_tract_block_group` from cell[19]
- [ ] Create Supabase table DDL if needed

---

### 5. agg_tax_assessor

Aggregated tax assessor data by `nedl_property_id` (rollup of multiple parcels).

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/03_insertion_transform_tax_assessor_y_c_map_rent_datas_prod.ipynb`
- cell[43]: Multiple aggregation functions:
  - `get_agg_row()`: First row by `tax_assessor_id`, `year_built` sort
  - `get_sum_agg_cols()`: Sum of `assessed_value_total`, `building_sq_ft`, `units_count`, etc.
  - `get_least_year()`: Min of `year_built`, `effective_year_built`
  - `get_average_value()`: Avg of `assessed_value_improvements`
  - `get_highest_amount_agg_values()`: Max of `last_sale_amount`, `prior_sale_amount`
  - `get_mode_agg_value()`: Mode of `building_sq_ft_code`

**Output Fields:** 58 columns including:
- `nedl_property_id`, `nedl_property_id_pk`, `tax_assessor_id`, `identifier`
- Geographic: `situs_state`, `situs_county`, `fips_code`, `cbsa_name`, `msa_name`
- Address: `address`, `city`, `state`, `zip`
- Aggregated: `units_count` (sum), `building_sq_ft` (sum), `assessed_value_total` (sum)
- Derived: `fips_tract_block_group`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/agg_tax_assessor.py`
  - Read from `analytics.dim_property` or raw
  - Implement groupby `nedl_property_id` with aggregation functions from cell[43]
  - Sum columns: `assessed_value_total`, `building_sq_ft`, `units_count`, etc.
  - Min columns: `year_built`
  - Max columns: `last_sale_amount`
- [ ] Create Supabase table DDL for `analytics.agg_tax_assessor`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

### 6. agg_rental

Aggregated rent data by property and period.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Refresh/Final_agg_nedl_rent_actual_incremental_load_prod.ipynb`
- cell[6]: Join `raw_nedl_rent_actual` with `y_c_mapping` and `src_data_enrich`
- Filter to 2-year window: `record_date >= max_record_date - 2 years`
- Series: `'Rent Actual'`

**Output Fields:**
- `nedl_property_id`, `nedl_property_id_pk`
- `market_name`, `submarket_name`, `property_name`
- `series`, `record_date`, `record_values`
- `state`, `zip`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/agg_rental.py`
  - Read from `raw.yardi_rent_occupancy` where `series = 'Rent Actual'`
  - Join with `raw.yc_mapping` to get `nedl_property_id`
  - Filter to 2-year rolling window
  - Output to `analytics.agg_rental`
- [ ] Create Supabase table DDL for `analytics.agg_rental`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

### 7. agg_occupancy

Aggregated occupancy data by property and period.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Refresh/Final_agg_nedl_occupancy_incremental_load_prod.ipynb`
- cell[6]: Same pattern as agg_rental but with `series = 'Occupancy Rate'`

**Output Fields:**
- `nedl_property_id`, `nedl_property_id_pk`
- `market_name`, `submarket_name`, `property_name`
- `series`, `record_date`, `record_values`
- `state`, `zip`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/agg_occupancy.py`
  - Read from `raw.yardi_rent_occupancy` where `series = 'Occupancy Rate'`
  - Join with `raw.yc_mapping` to get `nedl_property_id`
  - Filter to 2-year rolling window
  - Output to `analytics.agg_occupancy`
- [ ] Create Supabase table DDL for `analytics.agg_occupancy`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

### 8. agg_demographic

Aggregated demographic data by property.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/07_cherre_src_agg_nedl_demographic_prod.ipynb`
- Joins demographics to properties via census tract
- Aggregates 150+ demographic fields per property

**Output Fields:**
- `nedl_property_id`, `tax_assessor_id`, `zip`
- All demographic fields from `raw.cherre_demographics`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/agg_demographic.py`
  - Read from `raw.cherre_demographics`
  - Join to properties via `tax_assessor_id`
  - Output to `analytics.agg_demographic`
- [ ] Create Supabase table DDL for `analytics.agg_demographic`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

### 9. src_data_enrich

Enriched property data combining Yardi + Cherre sources.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/05_Incremental_src_enrich_script_prod.ipynb`
- cell[14]: Merge `y_c_mapping` with `yardi_filter` on `property_id`
  ```python
  yardi_y_c_enrich_df = y_c_mapping[['nedl_property_id','property_id','nedl_property_id_pk','tax_assessor_id']].merge(yardi_filter, on='property_id', how='inner')
  ```

**Output Fields:**
- `nedl_property_id`, `nedl_property_id_pk`, `property_id`, `tax_assessor_id`
- From Yardi: `property_name`, `address`, `city`, `county`, `state`, `zip`
- From Yardi: `units`, `sqft`, `completion_date`, `latitude`, `longitude`
- From Yardi: `owner`, `owner_contact_*` fields

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/src_data_enrich.py`
  - Read from `raw.yc_mapping` and `raw.yardi_properties`
  - Join on `property_id`
  - Select enriched columns
  - Output to `analytics.src_data_enrich`
- [ ] Create Supabase table DDL for `analytics.src_data_enrich`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

### 10. src_data_enrich_loans

Enriched loan data with property linkage.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/05_Incremental_src_enrich_script_prod.ipynb`
- cell[21-28]: Read loans, filter current, deduplicate
- cell[32]: Merge with `y_c_mapping`:
  ```python
  checking_yardi_loans = yardi_loans_with_propid.merge(y_c_mapping, on='property_id', how='inner')
  ```
- cell[36]: Rename columns: `lender` → `lender_name`, `originator` → `originator_name`

**Output Fields:**
- `nedl_property_id`, `nedl_property_id_pk`, `property_id`, `tax_assessor_id`
- `property_name`, `zip`
- `loan_amount` (in dollars), `loan_origination_date`, `loan_maturity_date`
- `lender_name`, `originator_name`, `loan_status`

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/src_data_enrich_loans.py`
  - Read from `raw.yardi_loans`
  - Join with `raw.yc_mapping` on `property_id`
  - Rename columns per cell[36]
  - Output to `analytics.src_data_enrich_loans`
- [ ] Create Supabase table DDL for `analytics.src_data_enrich_loans`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

### 11. src_data_enrich_transactions

Enriched transaction data with Cherre recorder + property linkage.

**Source:** `cre-dashboard-cherre-refresh-scripts/02_UAT/Refresh/UAT_yardi_enrich_insertion_23_10_24.ipynb`
- cell[10897 area]: Join recorder with transform_tax_assessor:
  ```sql
  SELECT r.*, ta.nedl_property_id 
  FROM cherre_src.raw_nedl_recorder r 
  INNER JOIN cherre_src.transform_nedl_tax_assessor ta 
  ON r.tax_assessor_id = ta.tax_assessor_id
  ```

**Output Fields:**
- All `fact_transaction` fields plus:
- `nedl_property_id` (from tax assessor linkage)

**Status:** TODO

**Subtasks:**
- [ ] Create `src/analytics/src_data_enrich_transactions.py`
  - Read from `analytics.fact_transaction`
  - Join with `analytics.dim_property` to get `nedl_property_id`
  - Output to `analytics.src_data_enrich_transactions`
- [ ] Create Supabase table DDL for `analytics.src_data_enrich_transactions`
- [ ] Add build call in `src/flows/transform_analytics.py`

---

## Infrastructure

---

### Audit Table

Tracks incremental load history for each table.

**Source:** `cre-dashboard-cherre-refresh-scripts/03_Prod/Insert/demographic_raw_extraction.ipynb`
- cell[2269]: Insert audit record with `table_name`, `schema_name`, `insert_date`, row counts

**Output Fields:**
- `table_name`, `schema_name`, `insert_date`
- `before_count`, `after_count`, `new_rows`

**Status:** TODO

**Subtasks:**
- [ ] Create Supabase table DDL for `raw.etl_audit`
- [ ] Create utility function in `src/db.py` to log audit records
- [ ] Integrate audit logging into extract and transform flows

---

## Summary

| Layer | Total | Done | TODO |
|-------|-------|------|------|
| Raw | 13 | 4 | 9 |
| Analytics | 11 | 3 | 8 |
| Infrastructure | 1 | 0 | 1 |
| **Total** | **25** | **7** | **18** |

