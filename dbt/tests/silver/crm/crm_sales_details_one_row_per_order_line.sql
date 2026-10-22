-- Silver holds one row per order line: (order_number, product_key).
select order_number, product_key, count(*) as copies
from {{ ref('crm_sales_details') }}
group by order_number, product_key
having count(*) > 1
