select
    product_id,
    product_name,
    product_type
from {{ ref('products') }}