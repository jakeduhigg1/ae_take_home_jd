SELECT min(created_ts) as min_date

FROM {{ ref('stg_account_created') }}