{{ config(materialized='table') }}

with latest_delivery as (
    -- Bronze is append-only: one row per category, latest delivery wins
    select *
    from {{ source('bronze', 'erp_px_cat_g1v2') }}
    where id is not null
    qualify row_number() over (
        partition by id
        order by _ingested_at desc, _row_hash
    ) = 1
)

select
    trim(id)                                   as category_id,
    trim(cat)                                  as category,
    trim(subcat)                               as subcategory,
    upper(trim(maintenance))         as maintenance_required,
    _source_file,
    _ingest_date
from latest_delivery