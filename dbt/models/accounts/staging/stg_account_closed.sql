select *


from {{ source('accounts', 'account_closed') }}
