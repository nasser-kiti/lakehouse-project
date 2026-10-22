-- Every product has exactly one current version (null end_date).
select product_key, count_if(end_date is null) as open_versions
from {{ ref('crm_prd_info') }}
group by product_key
having count_if(end_date is null) <> 1