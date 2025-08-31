WITH staged AS (
  SELECT
    *
  FROM
    {{ ref('stg_breweries') }}
),
transformed AS (
  SELECT
    -- IDs and Names (renaming 'id' for clarity)
    id AS brewery_id,
    name,
    -- Standardize brewery_type using a CASE statement
    CASE
      WHEN brewery_type IN ('micro', 'nano') THEN 'microbrewery'
      WHEN brewery_type IN ('taproom', 'bar', 'beergarden') THEN 'taproom'
      WHEN brewery_type = 'brewpub' THEN 'brewpub'
      WHEN brewery_type = 'regional' THEN 'regional'
      WHEN brewery_type = 'contract' THEN 'contract'
      WHEN brewery_type = 'large' THEN 'large'
      ELSE 'Unknown'
    END AS brewery_type,
    -- Combine address fields into a single, clean street_address
    trim(concat_ws(' ', address_1, address_2, address_3)) AS street_address,
    city,
    state_province,
    postal_code,
    country,
    -- Format lat/lon to a consistent precision
    cast(longitude AS decimal(10, 6)) AS longitude,
    cast(latitude AS decimal(10, 6)) AS latitude,
    -- Format phone number to E.164 standard (+1XXXXXXXXXX)
    CASE
      WHEN length(phone) = 10 THEN '+1' || phone
      ELSE NULL
    END AS phone,
    website_url
  FROM
    staged
)
SELECT
  *
FROM
  transformed
