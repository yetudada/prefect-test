select
    transaction_id,
    transaction_ts,
    transaction_date,
    transaction_hour,
    account_id,
    customer_id,
    product_id,
    branch_id,
    merchant_id,
    amount,
    direction,
    signed_amount
from {{ ref('int_transactions_enriched') }}