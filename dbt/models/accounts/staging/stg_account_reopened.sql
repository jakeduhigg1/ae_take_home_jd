select *


from {{ source('accounts', 'account_reopened') }}
