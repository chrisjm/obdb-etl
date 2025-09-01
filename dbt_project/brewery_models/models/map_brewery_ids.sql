WITH stg_obdb AS (
  -- Get the master brewery data from the OBDB source
  SELECT
    brewery_id,
    name,
    street_address,
    city,
    state_province,
    country
  FROM
    {{ ref('dim_breweries') }}
),
stg_ba AS (
  -- Get the brewery data from the BA JSON source to be mapped
  SELECT
    ba_brewery_id,
    name,
    street_address,
    city,
    state_province,
    country
  FROM
    {{ ref('stg_ba_breweries') }}
),
-- Normalize key text fields for reliable joining
normalized_obdb AS (
  SELECT
    brewery_id,
    LOWER(TRIM(name)) AS name,
    LOWER(TRIM(city)) AS city,
    LOWER(TRIM(state_province)) AS state_province
  FROM
    stg_obdb
  WHERE
    TRIM(country) = 'United States' -- Use TRIM and the correct country name
),
normalized_ba AS (
  SELECT
    ba_brewery_id,
    LOWER(TRIM(name)) AS name,
    LOWER(TRIM(city)) AS city,
    LOWER(TRIM(state_province)) AS state_province
  FROM
    stg_ba
  WHERE
    TRIM(country) = 'United States' -- Use TRIM and the correct country name
),
-- Strategy 1: High-confidence match on exact name and state.
-- This is based on the query that successfully returned 4,434 rows.
name_state_matches AS (
  SELECT
    obdb.brewery_id,
    ba.ba_brewery_id,
    'exact_name_state' AS match_strategy
  FROM
    normalized_obdb AS obdb
    INNER JOIN normalized_ba AS ba ON obdb.name = ba.name
    AND obdb.state_province = ba.state_province
),
-- Strategy 2: For remaining records, try a fuzzy name match within the same city.
-- This is a medium-confidence match that finds slight name variations.
fuzzy_name_city_matches AS (
  SELECT
    obdb.brewery_id,
    ba.ba_brewery_id,
    'fuzzy_name_city' AS match_strategy
  FROM
    normalized_obdb AS obdb
    INNER JOIN normalized_ba AS ba ON obdb.state_province = ba.state_province
    AND obdb.city = ba.city
    AND jaro_winkler_similarity(obdb.name, ba.name) > 0.90
  WHERE
    -- IMPORTANT: Exclude breweries that we've already matched in the first strategy
    obdb.brewery_id NOT IN (
      SELECT
        brewery_id
      FROM
        name_state_matches
    )
    AND ba.ba_brewery_id NOT IN (
      SELECT
        ba_brewery_id
      FROM
        name_state_matches
    )
)
SELECT
  *
FROM
  name_state_matches
UNION
ALL
SELECT
  *
FROM
  fuzzy_name_city_matches
