{#
    Branch performance with month-over-month growth. Only includes months
    that have at least one transaction in the window.
#}

with branch_customer_counts as (
    select
        a.branch_id,
        count(distinct a.customer_id) as customer_count
    from {{ ref('dim_accounts') }} a
    group by 1
),

monthly_volume as (
    select
        a.branch_id,
        date_trunc('month', t.transaction_date) as month,
        count(*) as txn_count,
        sum(t.amount) as txn_volume,
        sum(case when t.direction = 'credit' then t.amount else 0 end) as deposit_volume
    from {{ ref('fct_transactions') }} t
    join {{ ref('dim_accounts') }} a using (account_id)
    group by 1, 2
),

with_growth as (
    select
        branch_id,
        month,
        txn_count,
        txn_volume,
        deposit_volume,
        lag(txn_volume) over (partition by branch_id order by month) as prior_month_volume,
        case
            when lag(txn_volume) over (partition by branch_id order by month) is null then null
            when lag(txn_volume) over (partition by branch_id order by month) = 0 then null
            else (txn_volume - lag(txn_volume) over (partition by branch_id order by month))
                 / lag(txn_volume) over (partition by branch_id order by month)
        end as mom_growth_pct
    from monthly_volume
)

select
    b.branch_id,
    b.region,
    b.city,
    b.state,
    coalesce(cc.customer_count, 0) as customer_count,
    g.month,
    coalesce(g.txn_count, 0) as txn_count,
    coalesce(g.txn_volume, 0) as txn_volume,
    coalesce(g.deposit_volume, 0) as deposit_volume,
    g.mom_growth_pct
from {{ ref('dim_branches') }} b
left join branch_customer_counts cc using (branch_id)
left join with_growth g using (branch_id)