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

## Create a dev env from prod

```bash
sqlmesh plan dev --include-unmodified
```

## Deploy the DAG and conf in a local Airflow from Docker

```bash
docker run -it \
    -p 8080:8080 \
    -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
    -e GCP_PROJECT=gb-poc-373711 \
    -v $HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json \
    -v $(pwd)/world_cup_qatar_elt_sqlmesh_dag:/opt/airflow/dags/world_cup_qatar_elt_sqlmesh_dag \
    -v $(pwd)/models:/opt/airflow/dags/world_cup_qatar_elt_sqlmesh_project/models \
    -v $(pwd)/macros:/opt/airflow/dags/world_cup_qatar_elt_sqlmesh_project/macros \
    -v $(pwd)/seeds:/opt/airflow/dags/world_cup_qatar_elt_sqlmesh_project/seeds \
    -v $(pwd)/config.yaml:/opt/airflow/dags/world_cup_qatar_elt_sqlmesh_project/config.yaml \
    -v $(pwd)/config:/opt/airflow/config \
    airflow-dev
```

### Run the SQLMesh UI

Build:

```bash
docker build -t sqlmesh-ui -f sqlmesh_ui/Dockerfile .
```

Run:

```bash
docker run -it \
  -p 8000:8000 \
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
  -v $HOME/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json:ro \
  sqlmesh-ui
```
