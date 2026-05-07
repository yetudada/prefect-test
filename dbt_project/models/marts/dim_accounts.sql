select
    a.account_id,
    a.customer_id,
    a.product_id,
    p.product_name,
    p.product_type,
    a.branch_id,
    b.region as branch_region,
    b.city as branch_city,
    b.state as branch_state,
    a.opened_date,
    a.account_status,
    a.credit_limit
from {{ ref('stg_accounts') }} a
left join {{ ref('stg_products') }} p using (product_id)
left join {{ ref('stg_branches') }} b using (branch_id)