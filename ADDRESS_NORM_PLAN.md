# Address Normalization & Dimensional Model Implementation Plan

## Overview

This document outlines the implementation plan for transforming the OBDB-ETL pipeline from a flat brewery dataset to a dimensional model that separates brewery identity from physical locations. This enables tracking of address reuse over time (brewery succession at the same location) and brewery relocations.

**Project Context:**

- **v1 API** (separate repo): Community-driven, always-public OBDB dataset - managed separately with simplified contribution workflow
- **v2 API** (separate repo): Gold enriched dataset from this pipeline - API key protected, rate limited
- **This project**: Produces the gold dataset that v2 API will consume

## Goals

1. **Separate brewery identity from physical location** - Enable tracking of breweries and addresses independently
2. **Track location history** - Identify when breweries move or when new breweries occupy old locations
3. **Merge OBDB and BA datasets intelligently** - Use OBDB as source of truth with BA enrichment
4. **International address normalization** - Use libpostal for robust, international address parsing and standardization
5. **Flag ambiguous matches** - Create manual review queue for low-confidence matches
6. **Produce gold dataset** - Output enriched dataset for v2 API consumption (API implementation is separate)
7. **Track data provenance** - Maintain source attribution for transparency and data quality

## Architecture

### Data Model

```
┌─────────────────────┐
│  dim_addresses      │  ← Physical locations (normalized)
├─────────────────────┤
│ address_id (PK)     │
│ street_address      │
│ city                │
│ state_province      │
│ postal_code         │
│ country             │
│ latitude            │
│ longitude           │
│ coordinates_source  │  ← Provenance: obdb, ba, null
│ address_hash        │  ← MD5 of normalized components
│ normalized_address  │  ← libpostal output
│ is_public           │  ← Access control flag
│ first_seen_date     │
│ last_seen_date      │
└─────────────────────┘
         ▲
         │
         │ (FK: address_id)
         │
┌─────────────────────────┐
│  fact_brewery_locations │  ← SCD Type 2 bridge table
├─────────────────────────┤
│ location_id (PK)        │
│ gold_brewery_id (FK)    │
│ address_id (FK)         │
│ valid_from              │
│ valid_to                │  ← NULL = current
│ is_current              │
│ source_system           │  ← obdb, ba, both
│ match_confidence        │
└─────────────────────────┘
         │
         │ (FK: gold_brewery_id)
         ▼
┌─────────────────────┐
│  dim_breweries      │  ← Brewery identity (no address)
├─────────────────────┤
│ gold_brewery_id (PK)│
│ obdb_brewery_id     │
│ ba_brewery_id       │
│ name                │
│ brewery_type        │
│ phone               │
│ phone_source        │  ← Provenance: obdb, ba, null
│ website_url         │
│ website_source      │  ← Provenance: obdb, ba, null
│ ba_membership_status│
│ is_active           │
│ is_public           │  ← Access control flag
│ data_license        │  ← obdb, ba_public, ba_restricted
│ enrichment_date     │  ← When BA data was merged
│ first_seen_date     │
│ last_seen_date      │
└─────────────────────┘
         │
         │
         ▼
┌─────────────────────┐
│ brewery_id_mapping  │  ← Simple lookup table
├─────────────────────┤
│ gold_brewery_id (PK)│
│ obdb_brewery_id     │
│ ba_brewery_id       │
│ match_strategy      │
│ match_confidence    │
│ created_at          │
└─────────────────────┘

┌──────────────────────┐
│ manual_review_queue  │  ← Flagged matches
├──────────────────────┤
│ review_id (PK)       │
│ obdb_brewery_id      │
│ ba_brewery_id        │
│ match_reason         │
│ name_similarity      │
│ address_similarity   │
│ review_status        │
│ created_at           │
└──────────────────────┘
```

### dbt Model Organization

```
dbt_project/brewery_models/models/
├── staging/
│   ├── stg_breweries.sql          (existing - OBDB staging)
│   ├── stg_ba_breweries.sql       (existing - BA staging)
│   └── stg_addresses.sql          (NEW - union addresses from both sources)
│
├── intermediate/
│   ├── int_address_deduplication.sql  (NEW - dedupe via hash + coords)
│   ├── int_brewery_matching.sql       (REFACTOR - from map_brewery_ids)
│   └── int_manual_review_flags.sql    (NEW - identify ambiguous matches)
│
└── marts/
    ├── core/                          (dimensional model - internal use)
    │   ├── dim_addresses.sql          (NEW - final address dimension)
    │   ├── dim_breweries.sql          (REFACTOR - identity + provenance)
    │   ├── brewery_id_mapping.sql     (NEW - ID lookup table)
    │   ├── fact_brewery_locations.sql (NEW - SCD Type 2 bridge)
    │   └── manual_review_queue.sql    (NEW - flagged matches)
    │
    └── gold/                          (v2 API outputs - external consumption)
        ├── vw_gold_breweries_current.sql    (NEW - current snapshot for v2 API)
        ├── vw_gold_brewery_history.sql      (NEW - location history for v2 API)
        └── vw_gold_address_succession.sql   (NEW - address reuse for v2 API)
```

**Note:** v1 API will be managed in a separate project with simplified community contribution workflow. This pipeline focuses solely on producing the gold dataset for v2 API consumption.

## Implementation Steps

### Phase 1: Infrastructure Setup

#### 1.1 Add libpostal Dependency

**File:** `pyproject.toml`

Add libpostal to dependencies:

```toml
dependencies = [
    # ... existing deps
    "postal>=1.1.10",  # Python bindings for libpostal
]
```

**Note:** libpostal requires system-level installation:

```bash
# macOS
brew install libpostal

# Linux (Ubuntu/Debian)
sudo apt-get install libpostal-dev

# Then install Python bindings
uv sync
```

#### 1.2 Create Address Normalization Utility

**File:** `extract/address_utils.py`

Functions to implement:

- `normalize_address(street, city, state, postal, country)` - Use libpostal to parse and normalize
- `create_address_hash(normalized_components)` - Generate MD5 hash for deduplication
- `calculate_distance(lat1, lon1, lat2, lon2)` - Haversine distance in meters
- `addresses_match(addr1, addr2, threshold_meters=50)` - Coordinate-based matching

### Phase 2: Staging Layer

#### 2.1 Create stg_addresses.sql

**Purpose:** Union all addresses from both sources with basic normalization

**Logic:**

```sql
WITH obdb_addresses AS (
  SELECT
    id as source_id,
    'obdb' as source_system,
    address_1 as street_address,
    city,
    state_province,
    postal_code,
    country,
    latitude,
    longitude
  FROM {{ source('raw', 'raw_obdb_breweries') }}
  WHERE address_1 IS NOT NULL
),
ba_addresses AS (
  SELECT
    Id as source_id,
    'ba' as source_system,
    BillingAddress.street as street_address,
    BillingAddress.city as city,
    BillingAddress.state as state_province,
    BillingAddress.postalCode as postal_code,
    BillingAddress.country as country,
    BillingAddress.latitude as latitude,
    BillingAddress.longitude as longitude
  FROM {{ source('raw', 'raw_ba_json_data') }}
  WHERE BillingAddress.street IS NOT NULL
)
SELECT * FROM obdb_addresses
UNION ALL
SELECT * FROM ba_addresses
```

**Output:** All addresses from both sources, unnormalized

### Phase 3: Intermediate Layer

#### 3.1 Create int_address_deduplication.sql

**Purpose:** Deduplicate addresses using hash + coordinate matching

**Matching Strategy (3-tier):**

1. **Exact hash match** - Normalized address string matches exactly
2. **Coordinate proximity** - Within 50m if both have coordinates
3. **Fuzzy match** - Same city/state + similar street name (Jaro-Winkler > 0.90)

**Logic:**

```sql
-- Step 1: Normalize addresses (call Python UDF or use SQL normalization)
-- Step 2: Create address hash
-- Step 3: Group by hash, then by coordinate proximity
-- Step 4: Assign unique address_id to each deduplicated address
```

**Output:** Deduplicated addresses with `address_id`

#### 3.2 Refactor int_brewery_matching.sql

**Purpose:** Match OBDB and BA breweries (refactor from existing `map_brewery_ids.sql`)

**Matching Strategies:**

1. **High confidence (auto-match):**

   - Exact name + state match
   - Jaro-Winkler > 0.95 + city + state match

2. **Medium confidence (manual review):**

   - Jaro-Winkler 0.85-0.95 + city + state match
   - Same address + different name (Jaro-Winkler > 0.70)

3. **Low confidence (skip):**
   - Below thresholds

**Output:**

- Matched brewery pairs with confidence scores
- Flagged matches for manual review

#### 3.3 Create int_manual_review_flags.sql

**Purpose:** Identify ambiguous matches requiring human review

**Criteria for flagging:**

- Name similarity 0.85-0.95 (borderline)
- Same address but different names (possible name change vs succession)
- Multiple BA breweries matching single OBDB brewery
- Multiple OBDB breweries matching single BA brewery

**Output:** List of brewery pairs needing manual review

### Phase 4: Marts Layer

#### 4.1 Create dim_addresses.sql

**Purpose:** Final address dimension with all metadata

**Fields:**

- `address_id` (PK) - Sequential integer
- `street_address` - Original street address
- `city`, `state_province`, `postal_code`, `country`
- `latitude`, `longitude`
- `address_hash` - MD5 of normalized components
- `normalized_address` - libpostal normalized string
- `first_seen_date` - Earliest appearance in data
- `last_seen_date` - Latest appearance in data

**Logic:**

```sql
SELECT
  ROW_NUMBER() OVER (ORDER BY address_hash) as address_id,
  street_address,
  city,
  state_province,
  postal_code,
  country,
  latitude,
  longitude,
  address_hash,
  normalized_address,
  MIN(ingested_at) as first_seen_date,
  MAX(ingested_at) as last_seen_date
FROM {{ ref('int_address_deduplication') }}
GROUP BY address_hash, street_address, city, state_province,
         postal_code, country, latitude, longitude, normalized_address
```

#### 4.2 Create brewery_id_mapping.sql

**Purpose:** ID lookup table (OBDB as source of truth)

**ID Generation Logic:**

```sql
CASE
  -- OBDB brewery (matched or unmatched)
  WHEN obdb_brewery_id IS NOT NULL
    THEN 'obdb_' || obdb_brewery_id

  -- BA-only brewery
  WHEN ba_brewery_id IS NOT NULL
    THEN 'ba_' || ba_brewery_id

  -- Fallback (shouldn't happen)
  ELSE 'gen_' || MD5(name || city || state)
END as gold_brewery_id
```

**Fields:**

- `gold_brewery_id` (PK)
- `obdb_brewery_id` (nullable)
- `ba_brewery_id` (nullable)
- `match_strategy` (exact_name_state, fuzzy_name_city, null)
- `match_confidence` (0.0-1.0)
- `created_at`

#### 4.3 Refactor dim_breweries.sql

**Purpose:** Brewery identity dimension (NO address fields)

**Fields:**

- `gold_brewery_id` (PK) - From brewery_id_mapping
- `obdb_brewery_id` - Original OBDB ID
- `ba_brewery_id` - Original BA ID
- `name` - Brewery name (prefer OBDB)
- `brewery_type` - Type (prefer OBDB)
- `phone` - Phone number (prefer BA, fallback OBDB)
- `phone_source` - Provenance: 'obdb', 'ba', NULL
- `website_url` - Website (prefer BA, fallback OBDB)
- `website_source` - Provenance: 'obdb', 'ba', NULL
- `ba_membership_status` - BA-specific field
- `is_active` - Currently operating
- `is_public` - Can be exposed publicly (OBDB-only = TRUE, BA-only = FALSE)
- `data_license` - 'obdb', 'ba_public', 'ba_restricted'
- `enrichment_date` - When BA data was merged
- `first_seen_date` - First appearance
- `last_seen_date` - Last appearance

**Field Priority Rules:**
| Field | Priority | Rationale |
|-------|----------|-----------|
| name | OBDB | More standardized |
| brewery_type | OBDB | Consistent taxonomy |
| phone | BA → OBDB | BA more current |
| website_url | BA → OBDB | BA more current |
| coordinates | (in address table) | N/A |
| is_public | OBDB-only = TRUE, BA-only = FALSE | Licensing restrictions |

#### 4.4 Create fact_brewery_locations.sql

**Purpose:** SCD Type 2 bridge table linking breweries to addresses

**Fields:**

- `location_id` (PK) - Sequential
- `gold_brewery_id` (FK) - Links to dim_breweries
- `address_id` (FK) - Links to dim_addresses
- `valid_from` - Start date (initially CURRENT_DATE)
- `valid_to` - End date (NULL = current)
- `is_current` - Boolean flag
- `source_system` - obdb, ba, both
- `match_confidence` - 0.0-1.0

**Initial Load Logic:**

```sql
-- All current records
SELECT
  ROW_NUMBER() OVER () as location_id,
  m.gold_brewery_id,
  a.address_id,
  CURRENT_DATE as valid_from,
  NULL as valid_to,
  TRUE as is_current,
  CASE
    WHEN m.obdb_brewery_id IS NOT NULL AND m.ba_brewery_id IS NOT NULL THEN 'both'
    WHEN m.obdb_brewery_id IS NOT NULL THEN 'obdb'
    ELSE 'ba'
  END as source_system,
  COALESCE(m.match_confidence, 1.0) as match_confidence
FROM {{ ref('brewery_id_mapping') }} m
JOIN {{ ref('dim_breweries') }} b ON m.gold_brewery_id = b.gold_brewery_id
JOIN {{ ref('dim_addresses') }} a ON (
  -- Address matching logic here
)
```

**Future Updates (via git history backfill):**

- Set `valid_to` when brewery disappears from dataset
- Set `is_current = FALSE` for historical records
- Create new rows when brewery moves (new address_id)

#### 4.5 Create manual_review_queue.sql

**Purpose:** Flagged matches requiring human review

**Fields:**

- `review_id` (PK)
- `obdb_brewery_id`
- `ba_brewery_id`
- `obdb_name`, `ba_name`
- `obdb_address`, `ba_address`
- `match_reason` (borderline_name, same_address_diff_name, multiple_matches)
- `name_similarity` (Jaro-Winkler score)
- `address_similarity` (distance in meters or hash match)
- `review_status` (pending, approved, rejected)
- `created_at`

**Logic:**

```sql
SELECT
  ROW_NUMBER() OVER () as review_id,
  obdb.id as obdb_brewery_id,
  ba.Id as ba_brewery_id,
  obdb.name as obdb_name,
  ba.Name as ba_name,
  obdb.address_1 as obdb_address,
  ba.BillingAddress.street as ba_address,
  CASE
    WHEN jaro_winkler_similarity(obdb.name, ba.Name) BETWEEN 0.85 AND 0.95
      THEN 'borderline_name'
    WHEN address_match AND jaro_winkler_similarity(obdb.name, ba.Name) < 0.85
      THEN 'same_address_diff_name'
    ELSE 'multiple_matches'
  END as match_reason,
  jaro_winkler_similarity(obdb.name, ba.Name) as name_similarity,
  -- address similarity calculation
  'pending' as review_status,
  CURRENT_TIMESTAMP as created_at
FROM {{ ref('int_manual_review_flags') }}
```

#### 4.6 Create vw_current_breweries.sql

R - current brewery snapshot
**Purpose:** Gold dataset current snapshot for v2 API consumption (replaces `dim_breweries_combined.sql`)

**Logic:**

````sql
-- Current brewery locations (denormalized view for v2 API)
SELECT
  b.gold_brewery_id,
  b.obdb_brewery_id,
  b.ba_brewery_id,
  b.name,
  b.brewery_type,
  a.street_address,
  a.city,
  a.state_pl_code,
  a.coun
  b.phone,
  b.website_url,
  b.websitet
  a.longitude,
  a.coordinates_source,
  b.ba_membership_status,
  bl.source_
  b.first_seen_date,
  b.last_seen_date
FROM {{ ref('dim_breweries') }} b
JOIN {{ ref('fact_brewery_locations') }} bl
  ON b.gold_brewery_id = bl.gold_brewery_id
JOIN {{ ref('dim_addresses') }} a
  ON bl.address_id = a.address_id
WHERE bl.is_current = TRUE
``` irSress_id = bl.address_id
JOIN {{ ref('dim_breweries') }} b
  ON bl.gold_brewery_id = b.gold_brewery_id
ORDER BY a.address_id, bl.valid_from
````

### Phase 5: Testing

#### 5.1 dbt Tests

**File:** `models/marts/marts.yml`

```yaml
version: 2

models:
  - name: dim_addresses
    description: "Physical brewery locations (normalized)"
    columns:
      - name: address_id
        tests:
          - unique
          - not_null
      - name: address_hash
        tests:
          - not_null

  - name: dim_breweries
    description: "Brewery identity (no address)"
    columns:
      - name: gold_brewery_id
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null

  - name: brewery_id_mapping
    description: "ID lookup table"
    columns:
      - name: gold_brewery_id
        tests:
          - unique
          - not_null

  - name: fact_brewery_locations
    description: "Brewery-address relationships (SCD Type 2)"
    columns:
      - name: location_id
        tests:
          - unique
          - not_null
      - name: gold_brewery_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_breweries')
              field: gold_brewery_id
      - name: address_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_addresses')
              field: address_id
    tests:
      - dbt_utils.expression_is_true:
          expression: "valid_from <= COALESCE(valid_to, '9999-12-31')"
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - gold_brewery_id
            - valid_from
```

#### 5.2 Custom Tests

- Verify no duplicate current locations per brewery
- Verify address deduplication (no near-duplicate addresses)
- Verify ID mapping completeness (all breweries have gold_id)
- Verify manual review queue has valid brewery IDs

### Phase 6: Migration

#### 6.1 Update dbt_project.yml

**File:** `dbt_project/brewery_models/dbt_project.yml`

```yaml
models:
  brewery_models:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
      +schema: intermediate
    marts:
      core:
        +materialized: table
        +schema: marts_core
      gold:
        +materialized: view
        +schema: marts_gold
```

#### 6.2 Deprecate Old Models

- Remove `dim_breweries_combined.sql` (replaced by `vw_gold_breweries_current.sql`)
- Keep `map_brewery_ids.sql` temporarily for reference, then remove

#### 6.3 Update DAG

**File:** `dags/brewery_pipeline_dag.py`

No changes needed - dbt will automatically run new models

#### 6.4 Gold Dataset Export (Future)

For v2 API consumption, the gold dataset views can be exported as:

- JSON files for static hosting
- Database views for direct API queries
- Parquet files for analytics
  (API implementation is handled in separate repository)

## Usage Examples

### Query 1: Current Brewery Snapshot (v2 API)

```sql
-- Gold dataset current snapshot
SELECT * FROM vw_gold_breweries_current
WHERE is_active = TRUE
```

### Query 2: Brewery Succession at Address (v2 API)

```sql
-- Find all breweries that occupied a specific address
SELECT *
FROM vw_gold_address_succession
WHERE address_id = 'some_address_id'
ORDER BY succession_order
```

### Query 3: Brewery Relocations (v2 API)

```sql
-- Find breweries that moved locations
SELECT
  gold_brewery_id,
  name,
  COUNT(DISTINCT address_id) as location_count,
  STRING_AGG(city || ', ' || state_province, ' → '
    ORDER BY valid_from) as location_history
FROM vw_gold_brewery_history
GROUP BY gold_brewery_id, name
HAVING COUNT(DISTINCT address_id) > 1
```

### Query 4: Manual Review Queue

```sql
-- Get pending matches for review
SELECT
  obdb_name,
  ba_name,
  name_similarity,
  match_reason,
  obdb_address,
  ba_address
FROM manual_review_queue
WHERE review_status = 'pending'
ORDER BY name_similarity DESC
```

### Query 5: Address Reuse Analysis (v2 API)

```sql
-- Find addresses with multiple breweries over time
SELECT
  address_id,
  street_address,
  city,
  state_province,
  COUNT(DISTINCT gold_brewery_id) as brewery_count,
  STRING_AGG(brewery_name, ' → ' ORDER BY succession_order) as brewery_succession
FROM vw_gold_address_succession
GROUP BY address_id, street_address, city, state_province
HAVING COUNT(DISTINCT gold_brewery_id) > 1
ORDER BY brewery_count DESC
```

### Query 6: Data Provenance Analysis

```sql
-- Analyze data source distribution
SELECT
  source_system,
  COUNT(*) as brewery_count,
  COUNT(CASE WHEN phone_source = 'ba' THEN 1 END) as ba_phone_count,
  COUNT(CASE WHEN website_source = 'ba' THEN 1 END) as ba_website_count,
  COUNT(CASE WHEN is_public = TRUE THEN 1 END) as public_count
FROM vw_gold_breweries_current
GROUP BY source_system
```

## Future Enhancements

### Git History Backfill

**Purpose:** Populate historical `valid_to` dates and track closures

**Approach:**

1. Parse git log for CSV file changes
2. Extract brewery records from each commit
3. Identify when breweries disappear (closure)
4. Identify when breweries reappear (reopening)
5. Update `fact_brewery_locations` with historical dates

**Script:** `scripts/backfill_from_git_history.py`

### Data Quality Dashboard

- Match confidence distribution
- Manual review queue size
- Address deduplication stats
- Brewery lifecycle metrics (avg lifespan, closure rate)

### API Integration

- Expose `vw_current_breweries` via REST API
- Provide historical lookup endpoints
- Enable manual review workflow via API

## Rollout Plan

### Phase 1: Development (Week 1)

- [ ] Install libpostal system dependency
- [ ] Add libpostal to pyproject.toml
- [ ] Create address_utils.py
- [ ] Build staging models

### Phase 2: Intermediate Models (Week 1-2)

- [ ] Build address deduplication
- [ ] Refactor brewery matching
- [ ] Create manual review flags

### Phase 3: Marts (Week 2)

- [ ] Build dim_addresses
- [ ] Build brewery_id_mapping
- [ ] Refactor dim_breweries
- [ ] Build fact_brewery_locations
- [ ] Build manual_review_queue
- [ ] Create vw_current_breweries

### Phase 4: Testing & Validation (Week 2-3)

- [ ] Add dbt tests
- [ ] Validate data quality
- [ ] Review manual review queue
- [ ] Compare output with old dim_breweries_combined

### Phase 5: Deployment (Week 3)

- [ ] Update dbt_project.yml
- [ ] Remove deprecated models
- [ ] Run full pipeline
- [ ] Document new schema

### Phase 6: Backfill (Week 4+)

- [ ] Create git history parser
- [ ] Backfill historical dates
- [ ] Validate temporal data

## Success Metrics

- **Address deduplication rate:** >95% of duplicate addresses merged
- **Brewery matching rate:** >80% of BA breweries matched to OBDB
- **Manual review queue size:** <500 flagged matches
- **Data quality:** All dbt tests pass
- **Performance:** Pipeline completes in <10 minutes
- **Coverage:** All breweries from both sources represented

## Questions & Decisions Log

| Question                      | Decision                                               | Rationale                             |
| ----------------------------- | ------------------------------------------------------ | ------------------------------------- |
| Address normalization library | libpostal                                              | International support needed          |
| Coordinate matching threshold | 50 meters                                              | Reasonable for GPS accuracy           |
| Name mismatch handling        | Flag for manual review                                 | Avoid false positives                 |
| Backward compatibility        | Replace dim_breweries_combined                         | First-time implementation             |
| ID strategy                   | OBDB as source of truth                                | Stable, public dataset                |
| v1 vs v2 separation           | v1 in separate repo, this produces gold dataset for v2 | Clean separation of concerns          |
| BA data access                | Restricted behind API key in v2 API                    | Licensing restrictions                |
| v2 schema structure           | Nested objects, different from v1                      | Modern API design                     |
| Historical data backfill      | Handle at later date                                   | Focus on current implementation first |
| Data provenance tracking      | Yes, track source for all enriched fields              | Transparency and data quality         |

## References

- [libpostal GitHub](https://github.com/openvenues/libpostal)
- [dbt SCD Type 2 Pattern](https://docs.getdbt.com/blog/scd-in-dbt)
- [Dimensional Modeling Best Practices](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
