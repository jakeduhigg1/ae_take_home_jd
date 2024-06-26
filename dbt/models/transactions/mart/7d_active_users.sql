{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH base AS (
    SELECT 
        date,
        user_id_hashed,
        1 AS has_account_open,
        SUM(transactions_last_7_days) AS transactions_last_7_days
    FROM {{ ref('int_daily_account_transactions') }}

    GROUP BY 1,2,3
)

{% if is_incremental() %}

-- Incremental logic: only get new records
SELECT 
    date,
    user_id_hashed,
    has_account_open,
    CASE WHEN transactions_last_7_days > 0 THEN 1 ELSE 0 END AS is_7d_active_user 
FROM base
WHERE date > (SELECT MAX(date) FROM {{ this }})

{% else %}

-- Full refresh logic: get all records
SELECT 
    date,
    user_id_hashed,
    has_account_open,
    CASE WHEN transactions_last_7_days > 0 THEN 1 ELSE 0 END AS is_7d_active_user 
FROM base

{% endif %}