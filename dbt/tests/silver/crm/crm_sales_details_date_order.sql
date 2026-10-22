-- An order can't ship before it was placed, or be due before it ships.
select * from {{ ref('crm_sales_details') }}
where order_date > shipping_date or shipping_date > due_date