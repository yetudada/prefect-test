select
    t.transaction_id,
    t.transaction_ts,
    t.transaction_date,
    t.transaction_hour,
    t.amount,
    t.direction,
    t.signed_amount,
    a.account_id,
    a.customer_id,
    a.product_id,
    a.branch_id,
    a.account_status,
    a.credit_limit,
    p.product_name,
    p.product_type,
    m.merchant_id,
    m.merchant_name,
    m.mcc_code,
    m.country as merchant_country,
    m.is_foreign as merchant_is_foreign
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_accounts') }} a using (account_id)
left join {{ ref('stg_products') }} p using (product_id)
left join {{ ref('stg_merchants') }} m using (merchant_id)