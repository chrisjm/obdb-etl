WITH dim_obdb AS (
  SELECT
    *
  FROM
    {{ ref('dim_breweries') }}
),
stg_ba AS (
  SELECT
    *
  FROM
    {{ ref('stg_ba_breweries') }}
),
brewery_mapping AS (
  SELECT
    *
  FROM
    {{ ref('map_brewery_ids') }}
),
matched_breweries AS (
  SELECT
    obdb.brewery_id,
    ba.ba_brewery_id,
    obdb.name,
    obdb.brewery_type,
    obdb.street_address,
    obdb.city,
    obdb.state_province,
    obdb.postal_code,
    obdb.country,
    COALESCE(obdb.phone, ba.phone) AS phone,
    COALESCE(obdb.website_url, ba.website_url) AS website_url,
    obdb.longitude,
    obdb.latitude,
    map.match_strategy,
    'matched' AS source_status
  FROM
    dim_obdb AS obdb
    INNER JOIN brewery_mapping AS map ON obdb.brewery_id = map.brewery_id
    LEFT JOIN stg_ba AS ba ON map.ba_brewery_id = ba.ba_brewery_id
),
unmatched_obdb AS (
  SELECT
    brewery_id,
    NULL AS ba_brewery_id,
    name,
    brewery_type,
    street_address,
    city,
    state_province,
    postal_code,
    country,
    phone,
    website_url,
    longitude,
    latitude,
    NULL AS match_strategy,
    'obdb_only' AS source_status
  FROM
    dim_obdb
  WHERE
    brewery_id NOT IN (
      SELECT
        brewery_id
      FROM
        brewery_mapping
    )
),
unmatched_ba AS (
  SELECT
    ba_brewery_id AS brewery_id,
    ba_brewery_id,
    name,
    brewery_type,
    street_address,
    city,
    state_province,
    postal_code,
    country,
    phone,
    website_url,
    longitude,
    latitude,
    NULL AS match_strategy,
    'ba_only' AS source_status
  FROM
    stg_ba
  WHERE
    ba_brewery_id NOT IN (
      SELECT
        ba_brewery_id
      FROM
        brewery_mapping
    )
)
SELECT
  *
FROM
  matched_breweries
UNION
ALL
SELECT
  *
FROM
  unmatched_obdb
UNION
ALL
SELECT
  *
FROM
  unmatched_ba
