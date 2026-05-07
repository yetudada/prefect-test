{#
    Bucket customers into Mass / Mass Affluent / Affluent using behavioural
    signals from the last 90 days, NOT the registered_segment from signup.
    Thresholds are illustrative — real segmentation would tune these against
    actual portfolios.
#}

with latest_balances as (
    select
        customer_id,
        sum(running_balance) as total_balance
    from {{ ref('int_account_daily_balances') }}
    where balance_date = (select max(balance_date) from {{ ref('int_account_daily_balances') }})
    group by 1
),

recent_velocity as (
    select
        a.customer_id,
        count(*) as txn_count_90d
    from {{ ref('stg_transactions') }} t
    join {{ ref('stg_accounts') }} a using (account_id)
    where t.transaction_date >= (
        select max(transaction_date) - interval '90 days' from {{ ref('stg_transactions') }}
    )
    group by 1
),

joined as (
    select
        c.customer_id,
        coalesce(b.total_balance, 0) as total_balance,
        coalesce(v.txn_count_90d, 0) as txn_count_90d
    from {{ ref('stg_customers') }} c
    left join latest_balances b using (customer_id)
    left join recent_velocity v using (customer_id)
)

select
    customer_id,
    total_balance,
    txn_count_90d,
    case
        when total_balance >= 50000 and txn_count_90d >= 50 then 'Affluent'
        when total_balance >= 10000 or txn_count_90d >= 30 then 'Mass Affluent'
        else 'Mass'
    end as behavioural_segment
from joined