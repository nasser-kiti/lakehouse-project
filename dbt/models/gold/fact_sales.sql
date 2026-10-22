-- One row per order line. Products are joined point in time: the version valid on the order date.
select
    s.order_number,
    coalesce(p.product_sk, -1)                      as product_sk,
    xxhash64(s.customer_id)                         as customer_sk,
    s.order_date,
    s.shipping_date,
    s.due_date,
    s.quantity,
    s.price,
    s.order_total
from {{ ref('crm_sales_details') }} as s
left join {{ ref('dim_products') }} as p
    on  s.product_key = p.product_key
    and s.order_date between p.valid_from and p.valid_to