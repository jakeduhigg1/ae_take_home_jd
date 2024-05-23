select *


from {{ source('accounts', 'account_transactions') }}