# lakehouse-project


**Installing databricks on local machine**

```bash
brew tap databricks/tap
brew trust databricks/tap
brew install databricks
```

**Data**

| Table             | Rows  |
|-------------------|-------|
| _crm_cust_info    | 18494 |
| crm_prd_info      | 397   |
| crm_sales_details | 60398 |
| erp_cust_az12     | 18484 |
| erp_loc_a101      | 18484 |
| erp_px_cat_g1v2   | 37    |

Immutability
layer 1(Access): Only the ingestion servoce principal can write to the volume
layer 2(Processing): Before loading data, the loading service should verify the integrity of the landed files against their manifest - fail if any discrepancy is found.
layer 3(Storage): Retention policies should ensure that landed data is preserved for a defined period and cannot be modified or deleted within that period.