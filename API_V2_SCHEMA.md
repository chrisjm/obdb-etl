# API v2 Schema Design

## Overview

This document defines the proposed schema for the v2 API that will consume the gold dataset produced by this ETL pipeline. The v2 API is designed to be a modern, enriched dataset with BA data integration, historical tracking, and data provenance.

**Key Differences from v1:**

- Nested object structure (vs flat)
- Data provenance tracking
- Historical location data
- BA enrichment fields
- Access control (API key protected, rate limited)

## Core Endpoints

### 1. Current Breweries Snapshot

**Endpoint:** `GET /v2/breweries`

**Source:** `vw_gold_breweries_current`

**Response Schema:**

```json
{
  "data": [
    {
      "id": "obdb_abc123",
      "identifiers": {
        "obdb_id": "abc123",
        "ba_id": "xyz789"
      },
      "name": "Example Brewing Company",
      "brewery_type": "micro",
      "address": {
        "street": "123 Main Street",
        "city": "Portland",
        "state": "Oregon",
        "postal_code": "97201",
        "country": "United States"
      },
      "contact": {
        "phone": "503-555-1234",
        "phone_source": "ba",
        "website": "https://example.com",
        "website_source": "obdb"
      },
      "location": {
        "latitude": 45.5152,
        "longitude": -122.6784,
        "coordinates_source": "obdb"
      },
      "enrichment": {
        "ba_membership_status": "Active",
        "match_confidence": 0.98,
        "source_system": "both",
        "enrichment_date": "2026-01-10"
      },
      "metadata": {
        "is_active": true,
        "is_public": true,
        "data_license": "obdb",
        "first_seen": "2020-01-15",
        "last_seen": "2026-01-10"
      }
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 50,
    "total": 25000,
    "total_pages": 500
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

**Query Parameters:**

- `page` (int): Page number (default: 1)
- `per_page` (int): Results per page (default: 50, max: 100)
- `state` (string): Filter by state/province
- `city` (string): Filter by city
- `brewery_type` (string): Filter by type
- `is_active` (boolean): Filter by active status
- `source_system` (string): Filter by source (obdb, ba, both)

### 2. Single Brewery Detail

**Endpoint:** `GET /v2/breweries/{id}`

**Source:** `vw_gold_breweries_current`

**Response Schema:**

```json
{
  "data": {
    "id": "obdb_abc123",
    "identifiers": {
      "obdb_id": "abc123",
      "ba_id": "xyz789"
    },
    "name": "Example Brewing Company",
    "brewery_type": "micro",
    "address": {
      "street": "123 Main Street",
      "city": "Portland",
      "state": "Oregon",
      "postal_code": "97201",
      "country": "United States"
    },
    "contact": {
      "phone": "503-555-1234",
      "phone_source": "ba",
      "website": "https://example.com",
      "website_source": "obdb"
    },
    "location": {
      "latitude": 45.5152,
      "longitude": -122.6784,
      "coordinates_source": "obdb"
    },
    "enrichment": {
      "ba_membership_status": "Active",
      "match_confidence": 0.98,
      "source_system": "both",
      "enrichment_date": "2026-01-10"
    },
    "metadata": {
      "is_active": true,
      "is_public": true,
      "data_license": "obdb",
      "first_seen": "2020-01-15",
      "last_seen": "2026-01-10"
    }
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

### 3. Brewery Location History

**Endpoint:** `GET /v2/breweries/{id}/history`

**Source:** `vw_gold_brewery_history`

**Response Schema:**

```json
{
  "data": {
    "brewery_id": "obdb_abc123",
    "brewery_name": "Example Brewing Company",
    "locations": [
      {
        "address": {
          "street": "456 Old Street",
          "city": "Portland",
          "state": "Oregon",
          "postal_code": "97202",
          "country": "United States"
        },
        "location": {
          "latitude": 45.51,
          "longitude": -122.67
        },
        "valid_from": "2015-03-01",
        "valid_to": "2020-06-30",
        "is_current": false,
        "source_system": "obdb"
      },
      {
        "address": {
          "street": "123 Main Street",
          "city": "Portland",
          "state": "Oregon",
          "postal_code": "97201",
          "country": "United States"
        },
        "location": {
          "latitude": 45.5152,
          "longitude": -122.6784
        },
        "valid_from": "2020-07-01",
        "valid_to": null,
        "is_current": true,
        "source_system": "both"
      }
    ]
  },
  "meta": {
    "location_count": 2,
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

### 4. Address Succession (Brewery Turnover)

**Endpoint:** `GET /v2/addresses/{address_id}/succession`

**Source:** `vw_gold_address_succession`

**Response Schema:**

```json
{
  "data": {
    "address": {
      "id": "addr_12345",
      "street": "789 Brewery Lane",
      "city": "Denver",
      "state": "Colorado",
      "postal_code": "80202",
      "country": "United States",
      "latitude": 39.7392,
      "longitude": -104.9903
    },
    "breweries": [
      {
        "brewery_id": "obdb_old123",
        "brewery_name": "Old Brewery Co",
        "valid_from": "2010-01-01",
        "valid_to": "2018-12-31",
        "is_current": false,
        "succession_order": 1
      },
      {
        "brewery_id": "obdb_new456",
        "brewery_name": "New Brewery LLC",
        "valid_from": "2019-03-15",
        "valid_to": null,
        "is_current": true,
        "succession_order": 2
      }
    ]
  },
  "meta": {
    "brewery_count": 2,
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

### 5. Search Breweries

**Endpoint:** `GET /v2/breweries/search`

**Query Parameters:**

- `q` (string): Search query (name, city, state)
- `lat` (float): Latitude for proximity search
- `lon` (float): Longitude for proximity search
- `radius` (int): Radius in miles (default: 10)
- `page` (int): Page number
- `per_page` (int): Results per page

**Response Schema:** Same as endpoint #1 (list format)

### 6. Statistics & Analytics

**Endpoint:** `GET /v2/stats`

**Response Schema:**

```json
{
  "data": {
    "total_breweries": 25000,
    "active_breweries": 23500,
    "by_source": {
      "obdb_only": 5000,
      "ba_only": 15000,
      "matched": 5000
    },
    "by_type": {
      "micro": 15000,
      "brewpub": 5000,
      "regional": 3000,
      "large": 1000,
      "planning": 500,
      "contract": 300,
      "proprietor": 200
    },
    "by_country": {
      "United States": 20000,
      "Canada": 2000,
      "United Kingdom": 1500,
      "Germany": 1000,
      "Other": 500
    },
    "data_quality": {
      "with_coordinates": 22000,
      "with_phone": 18000,
      "with_website": 20000,
      "ba_enriched": 20000
    },
    "provenance": {
      "phone_from_ba": 12000,
      "phone_from_obdb": 6000,
      "website_from_ba": 10000,
      "website_from_obdb": 10000
    }
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "last_updated": "2026-01-10T18:00:00Z",
    "version": "2.0.0"
  }
}
```

## Field Definitions

### Core Fields

| Field                 | Type   | Description                   | Source Priority |
| --------------------- | ------ | ----------------------------- | --------------- |
| `id`                  | string | Gold brewery ID (primary key) | Generated       |
| `identifiers.obdb_id` | string | Original OBDB ID              | OBDB            |
| `identifiers.ba_id`   | string | Brewers Association ID        | BA              |
| `name`                | string | Brewery name                  | OBDB preferred  |
| `brewery_type`        | string | Type of brewery               | OBDB preferred  |

### Address Fields

| Field                 | Type   | Description     | Source Priority |
| --------------------- | ------ | --------------- | --------------- |
| `address.street`      | string | Street address  | OBDB preferred  |
| `address.city`        | string | City            | OBDB preferred  |
| `address.state`       | string | State/Province  | OBDB preferred  |
| `address.postal_code` | string | Postal/ZIP code | OBDB preferred  |
| `address.country`     | string | Country         | OBDB preferred  |

### Contact Fields

| Field                    | Type   | Description             | Source Priority             |
| ------------------------ | ------ | ----------------------- | --------------------------- |
| `contact.phone`          | string | Phone number            | BA preferred, OBDB fallback |
| `contact.phone_source`   | string | Data source for phone   | Provenance                  |
| `contact.website`        | string | Website URL             | BA preferred, OBDB fallback |
| `contact.website_source` | string | Data source for website | Provenance                  |

### Location Fields

| Field                         | Type   | Description                 | Source Priority        |
| ----------------------------- | ------ | --------------------------- | ---------------------- |
| `location.latitude`           | float  | Latitude                    | Best quality available |
| `location.longitude`          | float  | Longitude                   | Best quality available |
| `location.coordinates_source` | string | Data source for coordinates | Provenance             |

### Enrichment Fields

| Field                             | Type   | Description               | Source    |
| --------------------------------- | ------ | ------------------------- | --------- |
| `enrichment.ba_membership_status` | string | BA membership status      | BA only   |
| `enrichment.match_confidence`     | float  | Matching confidence (0-1) | Generated |
| `enrichment.source_system`        | string | Source system(s)          | Generated |
| `enrichment.enrichment_date`      | date   | When BA data was merged   | Generated |

### Metadata Fields

| Field                   | Type    | Description                 | Source    |
| ----------------------- | ------- | --------------------------- | --------- |
| `metadata.is_active`    | boolean | Currently operating         | Generated |
| `metadata.is_public`    | boolean | Publicly accessible data    | Generated |
| `metadata.data_license` | string  | Data license type           | Generated |
| `metadata.first_seen`   | date    | First appearance in dataset | Generated |
| `metadata.last_seen`    | date    | Last appearance in dataset  | Generated |

## Enum Values

### brewery_type

- `micro` - Microbrewery
- `brewpub` - Brewpub
- `regional` - Regional brewery
- `large` - Large brewery
- `planning` - Planning (not yet open)
- `contract` - Contract brewery
- `proprietor` - Proprietor

### source_system

- `obdb` - OBDB only
- `ba` - BA only
- `both` - Matched from both sources

### data_license

- `obdb` - Open Brewery DB license (public)
- `ba_public` - BA public data
- `ba_restricted` - BA restricted data (API key required)

### phone_source / website_source / coordinates_source

- `obdb` - From OBDB dataset
- `ba` - From BA dataset
- `null` - Not available

## Error Responses

### 404 Not Found

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Brewery not found",
    "details": "No brewery found with ID: obdb_abc123"
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

### 400 Bad Request

```json
{
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "Invalid query parameter",
    "details": "per_page must be between 1 and 100"
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

### 401 Unauthorized

```json
{
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Invalid or missing API key",
    "details": "Please provide a valid API key in the Authorization header"
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0"
  }
}
```

### 429 Rate Limit Exceeded

```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded",
    "details": "Maximum 100 requests per minute. Try again in 45 seconds."
  },
  "meta": {
    "generated_at": "2026-01-10T20:00:00Z",
    "version": "2.0.0",
    "retry_after": 45
  }
}
```

## Data Export Formats

The gold dataset can be exported in multiple formats for API consumption:

### 1. JSON Lines (JSONL)

```bash
# One brewery per line
{"id":"obdb_abc123","name":"Example Brewery",...}
{"id":"obdb_def456","name":"Another Brewery",...}
```

### 2. Parquet

- Efficient columnar storage
- Ideal for analytics and data warehousing
- Preserves nested structure

### 3. CSV (Flattened)

- Backward compatibility
- Simple tooling support
- Loses nested structure

### 4. Database Views

- Direct SQL access
- Real-time queries
- Ideal for API backend

## Migration from v1 to v2

### Field Mapping

| v1 Field         | v2 Field              | Notes                   |
| ---------------- | --------------------- | ----------------------- |
| `id`             | `identifiers.obdb_id` | v1 ID preserved         |
| `name`           | `name`                | Same                    |
| `brewery_type`   | `brewery_type`        | Same                    |
| `address_1`      | `address.street`      | Nested                  |
| `city`           | `address.city`        | Nested                  |
| `state_province` | `address.state`       | Nested                  |
| `postal_code`    | `address.postal_code` | Nested                  |
| `country`        | `address.country`     | Nested                  |
| `phone`          | `contact.phone`       | Nested, may be enriched |
| `website_url`    | `contact.website`     | Nested, may be enriched |
| `latitude`       | `location.latitude`   | Nested                  |
| `longitude`      | `location.longitude`  | Nested                  |

### New Fields in v2

- `id` (gold_brewery_id)
- `identifiers.ba_id`
- `contact.phone_source`
- `contact.website_source`
- `location.coordinates_source`
- `enrichment.*` (all fields)
- `metadata.*` (all fields)

## Implementation Notes

**For API developers (separate repo):**

1. **Data Source:** Query `vw_gold_breweries_current`, `vw_gold_brewery_history`, `vw_gold_address_succession` views from DuckDB
2. **Caching:** Implement Redis/Memcached for frequently accessed breweries
3. **Rate Limiting:** Use API gateway (e.g., Kong, Tyk) for rate limiting
4. **Authentication:** API key-based auth for BA-enriched data
5. **Pagination:** Cursor-based pagination for large result sets
6. **Search:** Consider Elasticsearch/Typesense for full-text search
7. **Monitoring:** Track usage by endpoint, source_system, data_license

## Versioning Strategy

- **v2.0.0** - Initial release with gold dataset
- **v2.1.0** - Add historical backfill data
- **v2.2.0** - Add additional BA enrichment fields
- **v3.0.0** - Breaking changes (if needed)

Semantic versioning: MAJOR.MINOR.PATCH

- MAJOR: Breaking changes
- MINOR: New features, backward compatible
- PATCH: Bug fixes, backward compatible
