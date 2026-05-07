select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    email,
    street_address,
    city,
    state,
    postal_code,
    signup_date,
    segment as registered_segment,
    kyc_status
from {{ ref('customers') }}