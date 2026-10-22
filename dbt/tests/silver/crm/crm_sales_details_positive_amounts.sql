select * from {{ ref('crm_sales_details') }}
where quantity <= 0 or price <= 0 or order_total <= 0
