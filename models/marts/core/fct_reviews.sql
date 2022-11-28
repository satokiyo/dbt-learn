{{
 config(
 materialized = 'incremental',
 on_schema_change='append_new_columns'
 )
}}
-- ignore/fail/append_new_columns/sync_all_columns

{% set columns = adapter.get_columns_in_relation(ref('stg_reviews')) %}
{% for column in columns %}
  {{ log("Column: " ~ column, info=true) }}
{% endfor %}

WITH stg_reviews AS (
 SELECT * FROM {{ ref('stg_reviews') }}
)
SELECT
  {{ dbt_utils.surrogate_key(["sr.listing_id", "reviewer_name", "review_date", "review_sentiment", "review_text"]) }} as review_pk,
 sr.*,
 dlc.listings_pk AS listings_fk


 FROM stg_reviews sr
 LEFT JOIN {{ ref("dim_listings_cleansed") }} dlc
 ON sr.listing_id = dlc.listing_id
WHERE review_text is not null
{% if is_incremental() %}
 AND review_date > (select max(review_date) from {{ this }})
{% endif %}
