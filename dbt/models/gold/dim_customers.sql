-- One row per customer. CRM is the master; ERP adds birth date and country, and gender when CRM has none.
select
    xxhash64(c.customer_id)                         as customer_sk,
    c.customer_id,
    c.customer_key,
    c.first_name,
    c.last_name,
    case
        when c.gender != 'n/a' then c.gender
        else coalesce(e.gender, 'n/a')
    end                                             as gender,
    c.marital_status,
    e.birth_date,
    coalesce(l.country, 'n/a')                      as country,
    c.created_at
from {{ ref('crm_cust_info') }} as c
left join {{ ref('erp_cust_az12') }} as e on c.customer_key = e.customer_key
left join {{ ref('erp_loc_a101') }}  as l on c.customer_key = l.customer_key