-- stg_breweries.sql
SELECT
  id,
  name,
  brewery_type,
  address_1,
  address_2,
  address_3,
  city,
  state_province,
  postal_code,
  country,
  phone,
  website_url,
  CASE
    WHEN latitude >= -90
    AND latitude <= 90 THEN CAST(latitude AS DECIMAL(10, 6))
    ELSE NULL
  END AS latitude,
  CASE
    WHEN longitude >= -180
    AND longitude <= 180 THEN CAST(longitude AS DECIMAL(10, 6))
    ELSE NULL
  END AS longitude
FROM
  {{ source('raw', 'raw_obdb_breweries') }}
