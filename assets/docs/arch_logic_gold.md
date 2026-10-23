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
