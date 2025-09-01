SELECT
  Id AS ba_brewery_id,
  Name AS name,
  Brewery_Type__c AS brewery_type,
  BillingAddress.street AS street_address,
  BillingAddress.city AS city,
  BillingAddress.state AS state_province,
  BillingAddress.postalCode AS postal_code,
  BillingAddress.country AS country,
  Phone AS phone,
  Website AS website_url,
  CAST(BillingAddress.latitude AS DECIMAL(10, 6)) AS latitude,
  CAST(BillingAddress.longitude AS DECIMAL(10, 6)) AS longitude,
  Membership_Record_Status__c AS ba_membership_status
FROM
  {{ source('raw', 'raw_json_data') }}
WHERE
  Is_Craft_Brewery__c IS TRUE
  AND Brewery_Type__c NOT IN (
    'Alt Prop',
    'Office only location',
    'Location',
    'Beer Brand'
  )
