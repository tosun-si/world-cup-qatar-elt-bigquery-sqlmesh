import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

SQLMESH_PROJECT_DIR = os.environ.get(
    "SQLMESH_PROJECT_DIR",
    "/opt/airflow/dags/world_cup_qatar_elt_sqlmesh_project"
)

with DAG(
        dag_id="world_cup_qatar_elt_sqlmesh_dag",
        default_args=default_args,
        schedule=None,
        catchup=False,
        description="World cup ELT DAG with SQLMesh"
) as dag:
    start = EmptyOperator(task_id="start")

    run_pipeline = BashOperator(
        task_id="run_sqlmesh_pipeline",
        bash_command=f"""
        set -e  # Exit immediately if a command fails

        echo "=============================="
        echo "🚀 Running SQLMesh Pipeline"
        echo "Working directory: {SQLMESH_PROJECT_DIR}"
        echo "=============================="

        cd {SQLMESH_PROJECT_DIR}

        echo "📂 Current directory: $(pwd)"
        echo "📄 Listing files:"
        ls -la

        echo "🧠 Running sqlmesh run..."
        # Run SQLMesh with debug logs for detailed visibility
        SQLMESH_LOG_LEVEL=DEBUG sqlmesh run 2>&1 || {{
            echo "=============================="
            echo "❌ SQLMesh run failed!"
            echo "Exit code: $?"
            echo "=============================="
            exit 1
        }}

        echo "=============================="
        echo "✅ SQLMesh pipeline completed successfully!"
        echo "=============================="
        """,
        append_env=True,
    )

    end = EmptyOperator(task_id="end")

    start >> run_pipeline >> end
