{{ config(materialized='table') }}

with latest_delivery as (
    -- Bronze is append-only: one row per customer, latest delivery wins
    select *
    from {{ source('bronze', 'erp_loc_a101') }}
    where cid is not null
    qualify row_number() over (
        partition by cid
        order by _ingested_at desc, _row_hash
    ) = 1
),

typed as (
    select
        replace(trim(cid), '-', '')   as customer_key,
        nullif(trim(cntry), '')       as raw_country,
        _source_file,
        _ingest_date
    from latest_delivery
)

select
    -- 'AW-00011000' -> 'AW00011000', to match crm_cust_info.customer_key
    customer_key,
    case
        when upper(raw_country) in ('US', 'USA', 'UNITED STATES') then 'United States'
        when upper(raw_country) in ('DE', 'GERMANY')              then 'Germany'
        when raw_country is null                                  then 'n/a'
        else raw_country
    end                                                           as country,
    _source_file,
    _ingest_date
from typed