-- No version ends before it starts.
select * from {{ ref('crm_prd_info') }}
where end_date < start_date