{#
    Per-customer per-month: txn count, net flow, ending balance, top spend
    category by transaction count.
#}

with monthly_txns as (
    select
        t.customer_id,
        date_trunc('month', t.transaction_date) as month,
        m.spend_category,
        count(*) as txn_count,
        sum(t.signed_amount) as net_flow
    from {{ ref('int_transactions_enriched') }} t
    left join {{ ref('int_merchants_categorized') }} m using (merchant_id)
    group by 1, 2, 3
),

monthly_aggregate as (
    select
        customer_id,
        month,
        sum(txn_count) as txn_count,
        sum(net_flow) as net_flow
    from monthly_txns
    group by 1, 2
),

top_category as (
    select
        customer_id,
        month,
        spend_category as top_spend_category
    from (
        select
            customer_id,
            month,
            spend_category,
            row_number() over (
                partition by customer_id, month
                order by txn_count desc, spend_category
            ) as rn
        from monthly_txns
    ) ranked
    where rn = 1
),

ending_balances as (
    select
        customer_id,
        date_trunc('month', balance_date) as month,
        sum(running_balance) as ending_balance
    from (
        select
            customer_id,
            balance_date,
            sum(running_balance) as running_balance,
            row_number() over (
                partition by customer_id, date_trunc('month', balance_date)
                order by balance_date desc
            ) as rn
        from {{ ref('int_account_daily_balances') }}
        group by 1, 2
    ) per_day
    where rn = 1
    group by 1, 2
)

select
    a.customer_id,
    a.month,
    a.txn_count,
    a.net_flow,
    eb.ending_balance,
    tc.top_spend_category
from monthly_aggregate a
left join top_category tc using (customer_id, month)
left join ending_balances eb using (customer_id, month)
order by 1, 2