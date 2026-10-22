-- Only sales without a valid order date may fall back to the unknown product.
select * from {{ ref('fact_sales') }}
where product_sk = -1 and order_date is not null