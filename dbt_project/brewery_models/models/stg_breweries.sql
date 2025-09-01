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
  longitude,
  latitude
FROM
  {{ source('raw', 'raw_obdb_breweries') }}
