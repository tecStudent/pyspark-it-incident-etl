import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

from src.airflow_orchestration import (
    append_airflow_run,
    inspect_landing,
    run_stage,
)
from src.pipeline_audit import (
    build_control_snapshot,
)


DAG_ID = "it_incident_incremental_pipeline"

PIPELINE_SCHEDULE = (
    os.getenv("AIRFLOW_PIPELINE_SCHEDULE")
    or None
)


@dag(
    dag_id=DAG_ID,
    description=(
        "Orquestra Bronze, Silver, Gold e "
        "reconciliação do pipeline incremental."
    ),
    start_date=datetime(
        2026,
        8,
        17,
        tzinfo=timezone.utc,
    ),
    schedule=PIPELINE_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(
            minutes=5,
        ),
    },
    tags=[
        "pyspark",
        "incremental",
        "incident-management",
    ],
    doc_md="""
    Pipeline incremental de incidentes de TI.

    Os dados permanecem no volume do projeto.
    O XCom transporta somente metadados pequenos
    de execução entre as tarefas.
    """,
)
def build_pipeline():
    @task(task_id="check_landing")
    def check_landing():
        result = inspect_landing()

        print(
            "Arquivos CSV encontrados: "
            f"{result['csv_count']}"
        )

        if result["files"]:
            print(
                "Arquivos disponíveis: "
                + ", ".join(result["files"])
            )
        else:
            print(
                "Nenhum CSV disponível. "
                "A execução idempotente continuará."
            )

        return result

    @task
    def execute_stage(
        stage_name: str,
    ):
        return run_stage(
            stage_name
        )

    @task(task_id="execution_summary")
    def execution_summary(
        landing_result,
        bronze_result,
        silver_result,
        gold_result,
        reconciliation_result,
    ):
        context = get_current_context()
        dag_run = context.get("dag_run")
        run_id = (
            dag_run.run_id
            if dag_run
            else str(context.get("run_id"))
        )

        stages = [
            bronze_result,
            silver_result,
            gold_result,
            reconciliation_result,
        ]

        run_record = {
            "dag_id": DAG_ID,
            "run_id": run_id,
            "status": "SUCCESS",
            "logical_date": str(
                context.get("logical_date")
            ),
            "landing": landing_result,
            "stages": stages,
            "duration_seconds": round(
                sum(
                    stage[
                        "duration_seconds"
                    ]
                    for stage in stages
                ),
                2,
            ),
            "control_snapshot": (
                build_control_snapshot()
            ),
        }

        audit_path = append_airflow_run(
            run_record
        )

        print(
            "Orquestração concluída com "
            "sucesso."
        )
        print(
            f"Auditoria Airflow: "
            f"{audit_path}"
        )

        return run_record

    landing = check_landing()

    bronze = execute_stage.override(
        task_id="bronze",
    )("bronze")

    silver = execute_stage.override(
        task_id="silver",
    )("silver")

    gold = execute_stage.override(
        task_id="gold",
    )("gold")

    reconciliation = (
        execute_stage.override(
            task_id="reconciliation",
        )("reconciliation")
    )

    summary = execution_summary(
        landing,
        bronze,
        silver,
        gold,
        reconciliation,
    )

    (
        landing
        >> bronze
        >> silver
        >> gold
        >> reconciliation
        >> summary
    )


build_pipeline()
