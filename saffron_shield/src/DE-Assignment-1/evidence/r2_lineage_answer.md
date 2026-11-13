# R2 Lineage Answer — tables reading `claims.raw.members.phone`

**Question:** if the member phone number were ever compromised, which downstream tables would be affected?

**Answer (traced from actual column-level lineage, not from memory):**

**None.** No table in the catalog consumes the `phone` column of `claims.raw.members`.

## Evidence

Query (run 2026-11-12, against `system.access.column_lineage`):

```sql
SELECT source_table_full_name, source_column_name,
       target_table_full_name, target_column_name
FROM system.access.column_lineage
WHERE source_table_full_name = 'claims.raw.members'
  AND source_column_name = 'phone'
```

Result: 3 rows, **all with NULL targets** — reads of the column are recorded, but the column itself is never written into any other table's column. So the full three-part names of tables reading `claims.raw.members.phone` is an empty list.

For contrast, table-level lineage (`system.access.table_lineage`) shows the pipeline does read the `members` **table**:

| source_table_full_name | target_table_full_name | entity_type |
| --- | --- | --- |
| claims.raw.members | claims.silver.claims_clean | PIPELINE |
| claims.raw.members | claims.silver.claims_quarantine | PIPELINE |

But column-level lineage confirms those targets derive from claim-event fields, not from `members.phone` (or any members PII column). `claims.gold.claims_enriched` and `claims.gold.hospital_monthly` do not appear anywhere in members lineage.

## Implication

A compromise of `claims.raw.members.phone` does **not** propagate through the lakehouse: no downstream table carries phone data. Exposure is limited to `claims.raw.members` itself — which is why the column is additionally protected by the `claims.gov.mask_last4` column mask (R1) and carries the `pii = 'true'` tag (R2).

*Caveat stated plainly: lineage reflects operations the platform has observed. A future pipeline that reads `members.phone` will appear here; re-run the query above as part of any recurring lineage review.*
