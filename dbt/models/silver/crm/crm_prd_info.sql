{{ config(materialized='table') }}

with latest_delivery as (
    -- Bronze is append-only: This CTE keeps the most recent delivery of each product version
    select *
    from {{ source('bronze', 'crm_prd_info') }}
    where prd_id is not null
    qualify row_number() over (
        partition by prd_id
        order by _ingested_at desc, _row_hash
    ) = 1
),

cleaned as (
    select
        cast(prd_id as int)                                  as product_id,
        replace(substring(trim(prd_key), 1, 5), '-', '_')    as category_id,
        substring(trim(prd_key), 7)                          as product_key,
        trim(prd_nm)                                         as product_name,
        cast(prd_cost as decimal(18, 2))                     as product_cost,
        case upper(trim(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'Other Sales'
            when 'T' then 'Touring'
            else 'n/a'
        end                                                  as product_line,
        try_to_date(prd_start_dt)                            as start_date,
        _source_file,
        _ingest_date
    from latest_delivery
)

select
    product_id,
    category_id,
    product_key,
    product_name,
    product_cost,
    product_line,
    start_date,
    date_sub(
        lead(start_date) over (partition by product_key order by start_date),
        1
    )                                                        as end_date,
    _source_file,
    _ingest_date
from cleaned