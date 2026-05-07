select
    account_id,
    customer_id,
    balance_date,
    net_flow,
    running_balance
from {{ ref('int_account_daily_balances') }}