{#
    Flag transactions that meet >= 2 of:
      - foreign merchant
      - amount > $5,000
      - off-hours (00:00-04:59)
      - > 2 standard deviations above the customer's 30-day average

    The 30-day baseline is computed once per customer over the last 30 days
    of the window — not a rolling per-transaction window — for clarity.
#}

with cust_30d as (
    select
        customer_id,
        avg(amount) as mean_amount_30d,
        stddev_pop(amount) as std_amount_30d
    from {{ ref('int_transactions_enriched') }}
    where transaction_date >= (
        select max(transaction_date) - interval '30 days'
        from {{ ref('stg_transactions') }}
    )
    group by 1
),

flagged as (
    select
        t.transaction_id,
        t.transaction_ts,
        t.transaction_date,
        t.transaction_hour,
        t.customer_id,
        t.account_id,
        t.merchant_id,
        t.amount,
        t.direction,
        t.merchant_country,
        coalesce(t.merchant_is_foreign, false) as flag_foreign,
        case when t.amount > 5000 then true else false end as flag_high_value,
        case when t.transaction_hour < 5 then true else false end as flag_off_hours,
        case
            when c.std_amount_30d is not null
                 and c.std_amount_30d > 0
                 and t.amount > c.mean_amount_30d + 2 * c.std_amount_30d
            then true
            else false
        end as flag_amount_outlier
    from {{ ref('int_transactions_enriched') }} t
    left join cust_30d c using (customer_id)
)

select
    transaction_id,
    transaction_ts,
    transaction_date,
    customer_id,
    account_id,
    merchant_id,
    amount,
    direction,
    merchant_country,
    flag_foreign,
    flag_high_value,
    flag_off_hours,
    flag_amount_outlier,
    (cast(flag_foreign as int)
     + cast(flag_high_value as int)
     + cast(flag_off_hours as int)
     + cast(flag_amount_outlier as int)) as risk_score
from flagged
where (cast(flag_foreign as int)
       + cast(flag_high_value as int)
       + cast(flag_off_hours as int)
       + cast(flag_amount_outlier as int)) >= 2