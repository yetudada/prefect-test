select
    transaction_id,
    account_id,
    merchant_id,
    transaction_ts,
    cast(transaction_ts as date) as transaction_date,
    extract(hour from transaction_ts) as transaction_hour,
    amount,
    direction,
    case when direction = 'credit' then amount else -amount end as signed_amount
from {{ ref('transactions') }}