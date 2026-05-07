select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name as full_name,
    c.date_of_birth,
    c.email,
    c.street_address,
    c.city,
    c.state,
    c.postal_code,
    c.signup_date,
    c.registered_segment,
    s.behavioural_segment,
    c.kyc_status,
    s.total_balance,
    s.txn_count_90d
from {{ ref('stg_customers') }} c
left join {{ ref('int_customer_segments') }} s using (customer_id)