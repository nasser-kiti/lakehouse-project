{{ config(materialized='table') }}

with latest_delivery as (
    -- Bronze is append-only: one row per customer, latest delivery wins
    select *
    from {{ source('bronze', 'erp_cust_az12') }}
    where cid is not null
    qualify row_number() over (
        partition by cid
        order by _ingested_at desc, _row_hash
    ) = 1
),

typed as (
    select
        upper(trim(cid))            as raw_cid,
        try_to_date(trim(bdate))    as raw_birth_date,
        upper(trim(gen))            as raw_gender,
        _source_file,
        _ingest_date
    from latest_delivery
)

select
    -- ERP prefixes some keys with 'NAS'; stripped so keys match crm_cust_info.customer_key
    case when raw_cid like 'NAS%' then substring(raw_cid, 4) else raw_cid end   as customer_key,
    -- Birth dates in the future are placeholders (e.g. 9999-11-20)
    case when raw_birth_date > current_date() then null else raw_birth_date end as birth_date,
    case
        when raw_gender in ('M', 'MALE')   then 'Male'
        when raw_gender in ('F', 'FEMALE') then 'Female'
        else 'n/a'
    end                                                                       as gender,
    _source_file,
    _ingest_date
from typed