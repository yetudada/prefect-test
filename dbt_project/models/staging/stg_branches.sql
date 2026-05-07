select
    branch_id,
    region,
    city,
    state,
    opened_date
from {{ ref('branches') }}