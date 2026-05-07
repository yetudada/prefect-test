{#
    Per-customer lifetime volume and a simple LTV tier. We don't generate
    explicit fee data, so fee_revenue is a 0.5% proxy of debit volume.
#}

with txn_volumes as (
    select
        customer_id,
        sum(case when direction = 'credit' then amount else 0 end) as deposit_volume,
        sum(case when direction = 'debit' then amount else 0 end) as debit_volume,
        count(*) as transaction_count
    from {{ ref('fct_transactions') }}
    group by 1
)

select
    c.customer_id,
    c.full_name,
    c.signup_date,
    c.behavioural_segment,
    coalesce(v.deposit_volume, 0) as deposit_volume,
    coalesce(v.debit_volume, 0) as debit_volume,
    coalesce(v.transaction_count, 0) as transaction_count,
    coalesce(v.debit_volume * 0.005, 0) as fee_revenue,
    coalesce(v.deposit_volume + v.debit_volume, 0) as lifetime_volume,
    case
        when coalesce(v.deposit_volume + v.debit_volume, 0) >= 50000 then 'Gold'
        when coalesce(v.deposit_volume + v.debit_volume, 0) >= 10000 then 'Silver'
        else 'Bronze'
    end as ltv_tier
from {{ ref('dim_customers') }} c
left join txn_volumes v using (customer_id)