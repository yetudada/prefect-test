select
    account_id,
    customer_id,
    product_id,
    branch_id,
    opened_date,
    status as account_status,
    credit_limit
from {{ ref('accounts') }}