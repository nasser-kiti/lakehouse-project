-- Silver must hold exactly one row per distinct non-null cst_id in Bronze.
-- Returns a row (and fails) if the counts differ.
with bronze as (
    select count(distinct cst_id) as n
    from {{ source('bronze', 'crm_cust_info') }}
    where cst_id is not null
),
silver as (
    select count(*) as n from {{ ref('crm_cust_info') }}
)
select bronze.n as bronze_customers, silver.n as silver_rows
from bronze cross join silver
where bronze.n <> silver.n