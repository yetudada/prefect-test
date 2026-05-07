{#
    Map merchants to spend categories via MCC ranges. Order matters: more
    specific MCC ranges (Dining 5811-5814, Cash 6010-6011) must be checked
    before the catch-all 'Other' bucket since they fall inside its range.
#}

select
    merchant_id,
    merchant_name,
    mcc_code,
    country,
    is_foreign,
    case
        when mcc_code between 5411 and 5499 then 'Groceries'
        when mcc_code between 5811 and 5814 then 'Dining'
        when mcc_code between 6010 and 6011 then 'Cash'
        when mcc_code between 3000 and 3299 then 'Travel'
        when mcc_code in (4511, 7011) then 'Travel'
        else 'Other'
    end as spend_category
from {{ ref('stg_merchants') }}