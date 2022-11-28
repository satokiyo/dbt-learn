WITH stg_hosts AS (
 SELECT
 *
 FROM
 {{ ref('stg_hosts') }}
)
SELECT
 {{ dbt_utils.surrogate_key(["created_at", "host_id", "host_name", "is_superhost", "updated_at"]) }} as hosts_pk,
 host_id,
 NVL(
 host_name,
 'Anonymous'
 ) AS host_name,
 is_superhost,
 created_at,
 updated_at
FROM
 stg_hosts
