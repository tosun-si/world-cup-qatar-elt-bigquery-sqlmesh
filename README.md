# world-cup-qatar-elt-bigquery-sqlmesh

This repo shows a real world use case with SQLMesh, BigQuery and Google Cloud. 
The raw and input data are represented by the Qatar Fifa World Cup Players stats, 
some transformations are applied with the ELT pattern and DBT to apply aggregation and business transformations.


## Cleanup the old env

```bash
sqlmesh janitor
```

By default, this:

- deletes expired snapshots and old environments (past TTL),
- removes orphaned temp tables,
- keeps only the active snapshot used by prod.

If you want to force cleanup immediately, run:

```bash
sqlmesh janitor --ignore-ttl
```