{{
  config(
    materialized = "table"
  )
}}

--accounts opened and yet to be closed
SELECT

    ao.user_id_hashed,
    ao.account_id_hashed,
    ao.account_type,
    "account_opened" AS event,
    ao.created_ts AS effective_from_ts,
    NULL AS effective_to_ts,
    1 AS latest_record_ind

FROM {{ ref('stg_account_created') }} AS ao

LEFT JOIN {{ ref('stg_account_closed') }} AS ac
    ON ao.account_id_hashed = ac.account_id_hashed

WHERE ac.account_id_hashed IS NULL

UNION ALL

--accounts opened and have since been closed
SELECT

    ao.user_id_hashed,
    ao.account_id_hashed,
    ao.account_type,
    "account_opened" AS event,
    ao.created_ts AS effective_from_ts,
    ac.closed_ts AS effective_to_ts,
    0 AS latest_record_ind

FROM {{ ref('stg_account_created') }} AS ao

INNER JOIN {{ ref('stg_account_closed') }} AS ac
    ON ao.account_id_hashed = ac.account_id_hashed

UNION ALL

--accounts closed and yet to be reopened
SELECT

    ao.user_id_hashed,
    ao.account_id_hashed,
    ao.account_type,
    "account_closed" AS event,
    ac.closed_ts AS effective_from_ts,
    NULL AS effective_to_ts,
    1 AS latest_record_ind

FROM {{ ref('stg_account_created') }} AS ao

INNER JOIN {{ ref('stg_account_closed') }} AS ac
    ON ao.account_id_hashed = ac.account_id_hashed

LEFT JOIN {{ ref('stg_account_reopened') }} AS ar
    ON ac.account_id_hashed = ar.account_id_hashed

WHERE ar.account_id_hashed IS NULL

UNION ALL

--accounts closed and have been reopened
SELECT

    ao.user_id_hashed,
    ao.account_id_hashed,
    ao.account_type,
    "account_closed" AS event,
    ac.closed_ts AS effective_from_ts,
    ar.reopened_ts AS effective_to_ts,
    0 AS latest_record_ind

FROM {{ ref('stg_account_created') }} AS ao

INNER JOIN {{ ref('stg_account_closed') }} AS ac
    ON ao.account_id_hashed = ac.account_id_hashed

INNER JOIN {{ ref('stg_account_reopened') }} AS ar
    ON ac.account_id_hashed = ar.account_id_hashed

UNION ALL

--accounts reopened
SELECT

    ao.user_id_hashed,
    ao.account_id_hashed,
    ao.account_type,
    "account_reopened" AS event,
    ar.reopened_ts AS effective_from_ts,
    NULL AS effective_to_ts,
    1 AS latest_record_ind

FROM {{ ref('stg_account_created') }} AS ao

INNER JOIN {{ ref('stg_account_closed') }} AS ac
    ON ao.account_id_hashed = ac.account_id_hashed

INNER JOIN {{ ref('stg_account_reopened') }} AS ar
    ON ac.account_id_hashed = ar.account_id_hashed

ORDER BY user_id_hashed ASC, account_id_hashed ASC, effective_from_ts ASC
