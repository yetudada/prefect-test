select
    merchant_id,
    merchant_name,
    mcc_code,
    country,
    case when country = 'USA' then false else true end as is_foreign
from {{ ref('merchants') }}