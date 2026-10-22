{{ config(materialized='table') }}

with latest_delivery as (
    -- Bronze is append-only: one row per order line, latest delivery wins
    select *
    from {{ source('bronze', 'crm_sales_details') }}
    qualify row_number() over (
        partition by sls_ord_num, sls_prd_key
        order by _ingested_at desc, _row_hash
    ) = 1
),

typed as (
    select
        trim(sls_ord_num)                            as order_number,
        trim(sls_prd_key)                            as product_key,
        cast(sls_cust_id as int)                     as customer_id,
        try_to_date(trim(sls_order_dt), 'yyyyMMdd')  as order_date,
        try_to_date(trim(sls_ship_dt), 'yyyyMMdd')   as shipping_date,
        try_to_date(trim(sls_due_dt), 'yyyyMMdd')    as due_date,
        cast(sls_quantity as int)                    as quantity,
        cast(sls_sales as decimal(18, 2))            as source_sales,
        cast(sls_price as decimal(18, 2))            as source_price,
        _source_file,
        _ingest_date
    from latest_delivery
),

priced as (
    select
        *,
        -- Business rule: price must be positive; derive it from sales when missing or zero
        case
            when source_price is null or source_price = 0
                then abs(source_sales) / nullif(quantity, 0)
            else abs(source_price)
        end as price
    from typed
)

select
    order_number,
    product_key,
    customer_id,
    case when order_date >= date'1990-01-01' then order_date end  as order_date,
    shipping_date,
    due_date,
    quantity,
    price,
    -- Business rule: sales = quantity × price. With price cleaned, this also
    -- replaces null, zero, negative and inconsistent source sales.
    quantity * price                                              as order_total,
    _source_file,
    _ingest_date
from priced