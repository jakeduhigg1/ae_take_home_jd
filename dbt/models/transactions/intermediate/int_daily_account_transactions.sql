SELECT DISTINCT

 DATE(date_day) as date
 ,accounts.user_id_hashed
 ,accounts.account_id_hashed
 ,IFNULL(tran.transactions_num,0) as transactions_num
 ,    SUM(IFNULL(tran.transactions_num,0)) OVER (
        PARTITION BY accounts.user_id_hashed, accounts.account_id_hashed
        ORDER BY DATE(date_day)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS transactions_last_7_days
 
 FROM {{ ref('dim_dates') }} as dates

 INNER JOIN {{ ref('accounts') }} as accounts
 ON DATE(dates.date_day) between DATE(accounts.effective_from_ts) and IFNULL(DATE(accounts.effective_to_ts),'2999-12-31')
 AND event IN ('account_opened','account_reopened')

 LEFT JOIN {{ ref('stg_account_transactions') }} as tran
 ON tran.date = DATE(dates.date_day)
 AND tran.account_id_hashed = accounts.account_id_hashed

 ORDER BY date asc
