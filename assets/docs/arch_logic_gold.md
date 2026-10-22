# Gold Architecture Decisions

| Decision | Choice | Why |
| ---------- | -------- | ----- |
| Model | Star schema: `fact_sales` with `dim_customers` and `dim_products` | Matches how the business asks questions (sales by customer, by product, over time) |
| Fact grain | One row per order line (`order_number`, product) | The lowest grain available; aggregates are built on top, never inside the fact |
| Surrogate keys | `xxhash64(natural key)`: `customer_id` for customers, `product_id` (one per version) for products | Deterministic across runs, unlike `row_number()`, which renumbers when rows are added; any model can derive the key without a join |
| Customer integration | CRM is the master; ERP adds birth date and country, and gender only when CRM has `'n/a'` | One source of truth per attribute; ERP customers missing from CRM are excluded |
| Product history | Point in time: one row per product version, sales joined on `order_date between valid_from and valid_to` | Each sale shows the product attributes valid when it happened |
| First product version | `valid_from` = 1990-01-01 (business start) for each product's earliest version | 18,410 sales (30%) were placed before their product's first recorded start date; a strict join would send them to the unknown member |
| Open versions | `valid_to` = 9999-12-31 for the current version | A closed range makes `between` work without null handling |
| Missing members | Unknown product row (`product_sk = -1`, `'n/a'` attributes); fact uses `coalesce(..., -1)` | 19 sales have no valid order date and match no version; they stay in totals instead of disappearing |
| Unknown customer | None | All 60,398 sales match a CRM customer; a `relationships` test at error severity fails the build if that ever changes |
| Missing category | Left join; `'n/a'` category and subcategory | Category `CO_PE` (7 pedal products) has no ERP match; those products have no sales today |
| Customer key in the fact | `xxhash64(customer_id)` computed from Silver, no join | Same hash on the same `int` column as the dimension |
| Materialisation | Views | Simple, nothing to store; the point-in-time join reruns per query, which is fine at 60k rows |
| Aggregates | Separate Gold models (`agg_*`), read by the Streamlit app | Report logic stays tested and versioned in dbt; the app only presents |
| Dimensions | Two (no date dimension) | Order dates are on the fact; a date dimension waits until reports need fiscal calendars or holidays |

## Tests

| Test | Catches |
| ------ | --------- |
| `unique` + `not_null` on `customer_sk`, `product_sk`, `customer_id` | Duplicate or missing keys |
| `relationships` from the fact to each dimension (error severity) | Facts pointing at a dimension row that doesn't exist |
| `fact_sales_row_count` (fact rows = Silver order lines) | Fan-out from the point-in-time join, and rows dropped by joins |
| `dim_products_no_overlapping_versions` | The cause of fan-out: two versions of a product valid on the same day |
| `fact_sales_unknown_product_only_without_date` | The unknown product being used for a sale that has a valid date (a missing version) |

## Known gap and planned work

**Write-Audit-Publish (planned).** dbt builds a Silver table before testing it, and Gold views read Silver directly, so data that fails a Silver test is visible through Gold until it is fixed. The job fails and Airflow goes red, so the failure is seen, but consumers aren't protected.

Chosen approach for when this is built: an **audit run, then a production run**. The dbt task first builds every layer into `*_audit` schemas with `--vars '{audit: true}'` (a `generate_schema_name` switch) and runs every test; only if all pass does a second command rebuild the real `silver` and `gold` schemas. Consumers get no access to the audit schemas. Cost: transformations run twice, which is seconds at this volume. At larger scale, publishing version-pinned views (`VERSION AS OF`) of the audited tables avoids the second run.
