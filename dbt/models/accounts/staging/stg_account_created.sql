select *


from {{ source('accounts', 'account_created') }}