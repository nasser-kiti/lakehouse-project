-- One row per product version (point in time), plus an unknown member (-1).
with versions as (
    select
        p.*,
        case
            when p.start_date = min(p.start_date) over (partition by p.product_key)
                then date'1990-01-01'
            else p.start_date
        end                                         as valid_from,
        coalesce(p.end_date, date'9999-12-31')      as valid_to
    from {{ ref('crm_prd_info') }} as p
)

select
    xxhash64(v.product_id)                          as product_sk,
    v.product_id,
    v.product_key,
    v.product_name,
    v.category_id,
    coalesce(c.category, 'n/a')                     as category,
    coalesce(c.subcategory, 'n/a')                  as subcategory,
    c.maintenance_required,
    v.product_line,
    v.product_cost,
    v.start_date,
    v.end_date,
    v.valid_from,
    v.valid_to,
    v.end_date is null                              as is_current
from versions as v
left join {{ ref('erp_px_cat_g1v2') }} as c on v.category_id = c.category_id

union all

select
    cast(-1 as bigint)      as product_sk,
    null                    as product_id,
    'n/a'                   as product_key,
    'n/a'                   as product_name,
    'n/a'                   as category_id,
    'n/a'                   as category,
    'n/a'                   as subcategory,
    null                    as maintenance_required,
    'n/a'                   as product_line,
    null                    as product_cost,
    null                    as start_date,
    null                    as end_date,
    null                    as valid_from,
    null                    as valid_to,
    false                   as is_current