-- Fact must have exactly one row per Silver order line: catches fan-out and dropped rows.
with silver as (select count(*) as n from {{ ref('crm_sales_details') }}),
     fact   as (select count(*) as n from {{ ref('fact_sales') }})
select silver.n as silver_rows, fact.n as fact_rows
from silver cross join fact
where silver.n <> fact.n