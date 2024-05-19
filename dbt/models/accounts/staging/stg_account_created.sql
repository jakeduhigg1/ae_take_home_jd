{{ config(materialized='view') }}

select

*

from {{ source('accounts', 'account_created') }}