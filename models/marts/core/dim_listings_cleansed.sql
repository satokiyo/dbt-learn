WITH stg_listings AS (
 SELECT
 *
 FROM
 {{ ref('stg_listings') }}
)
SELECT

 {{ dbt_utils.surrogate_key(["created_at", "host_id", "listing_id", "listing_name", "minimum_nights", "price_str", "room_type", "updated_at"]) }} as listings_pk,
 listing_id,
 listing_name,
 room_type,
 CASE
 WHEN minimum_nights = 0 THEN 1
 ELSE minimum_nights
 END AS minimum_nights,
 host_id,
 REPLACE(
 price_str,
 '$'
 ) :: NUMBER(
 10,
 2
 ) AS price,
 created_at,
 updated_at
FROM
 stg_listings

