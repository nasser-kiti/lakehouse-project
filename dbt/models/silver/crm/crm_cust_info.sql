{{ config(materialized='table') }}

with latest_delivery as (
    select *
    from {{ source('bronze', 'crm_cust_info') }}
    where cst_id is not null
    qualify row_number() over (
        partition by cst_id
        order by try_to_date(cst_create_date) desc nulls last,
                 _ingested_at desc,
                 _row_hash
    ) = 1
)

select
    cast(cst_id as int)                  as customer_id,
    trim(cst_key)                        as customer_key,
    trim(cst_firstname)                  as first_name,
    trim(cst_lastname)                   as last_name,
    case upper(trim(cst_marital_status))
        when 'M' then 'Married'
        when 'S' then 'Single'
        else 'n/a'
    end                                  as marital_status,
    case upper(trim(cst_gndr))
        when 'M' then 'Male'
        when 'F' then 'Female'
        else 'n/a'
    end                                  as gender,
    try_to_date(cst_create_date)         as created_at,
    _source_file,
    _ingest_date
from latest_delivery