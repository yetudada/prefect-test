select
    merchant_id,
    merchant_name,
    mcc_code,
    country,
    is_foreign,
    spend_category
from {{ ref('int_merchants_categorized') }}