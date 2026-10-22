-- Two versions of the same product must never be valid on the same day.
select a.product_key, a.product_id as version_a, b.product_id as version_b
from {{ ref('dim_products') }} as a
join {{ ref('dim_products') }} as b
  on  a.product_key = b.product_key
  and a.product_id < b.product_id
  and a.valid_from <= b.valid_to
  and b.valid_from <= a.valid_to
where a.product_sk <> -1