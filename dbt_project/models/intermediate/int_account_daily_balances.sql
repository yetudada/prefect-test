{#
    One row per account per day across the transaction window. Running balance
    is the cumulative sum of signed_amount (credits add, debits subtract).
    Densified using a date spine so days with no activity still have rows.
#}

with bounds as (
    select
        min(transaction_date) as window_start,
        max(transaction_date) as window_end
    from {{ ref('stg_transactions') }}
),

date_spine as (
    select cast(d as date) as balance_date
    from (
        select unnest(generate_series(
            (select window_start from bounds),
            (select window_end from bounds),
            interval '1 day'
        )) as d
    ) s
),

account_dates as (
    select
        a.account_id,
        a.customer_id,
        d.balance_date
    from {{ ref('stg_accounts') }} a
    cross join date_spine d
    where d.balance_date >= a.opened_date
),

daily_flow as (
    select
        account_id,
        transaction_date as balance_date,
        sum(signed_amount) as net_flow
    from {{ ref('stg_transactions') }}
    group by 1, 2
)

select
    ad.account_id,
    ad.customer_id,
    ad.balance_date,
    coalesce(df.net_flow, 0) as net_flow,
    sum(coalesce(df.net_flow, 0)) over (
        partition by ad.account_id
        order by ad.balance_date
        rows between unbounded preceding and current row
    ) as running_balance
from account_dates ad
left join daily_flow df
    on ad.account_id = df.account_id
    and ad.balance_date = df.balance_date