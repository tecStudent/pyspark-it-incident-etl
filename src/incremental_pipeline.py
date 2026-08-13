import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from src.pipeline_audit import (
    append_pipeline_run,
    build_control_snapshot,
)


STAGES = {
    "bronze": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "src/incremental_bronze.py",
    ],
    "silver": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "src/incremental_silver.py",
    ],
    "gold": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "src/incremental_gold.py",
    ],
    "reconciliation": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "src/pipeline_reconciliation.py",
    ],
}


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def run_stage(
    stage_name: str,
    command: list[str],
) -> float:
    print(
        f"\n{'=' * 60}\n"
        f"Executando etapa incremental: "
        f"{stage_name.upper()}\n"
        f"{'=' * 60}",
        flush=True,
    )

    start_time = time.time()

    subprocess.run(
        command,
        check=True,
    )

    elapsed = time.time() - start_time

    print(
        f"\nEtapa {stage_name.upper()} "
        f"concluída em {elapsed:.2f}s",
        flush=True,
    )

    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Executa o pipeline incremental "
            "de incidentes."
        )
    )

    parser.add_argument(
        "--from-stage",
        choices=list(STAGES.keys()),
        default="bronze",
        help=(
            "Etapa a partir da qual o pipeline "
            "será executado."
        ),
    )

    args = parser.parse_args()

    stage_names = list(STAGES.keys())

    start_index = stage_names.index(
        args.from_stage
    )

    selected_stages = stage_names[
        start_index:
    ]

    run_id = uuid4().hex
    started_at = utc_now()
    pipeline_start = time.time()

    status = "RUNNING"
    failed_stage = None
    error_message = None
    exit_code = 0
    stage_results = []

    try:
        for stage_name in selected_stages:
            stage_start = time.time()

            try:
                stage_elapsed = run_stage(
                    stage_name,
                    STAGES[stage_name],
                )

                stage_results.append(
                    {
                        "stage": stage_name,
                        "status": "SUCCESS",
                        "duration_seconds": round(
                            stage_elapsed,
                            2,
                        ),
                    }
                )

            except subprocess.CalledProcessError:
                stage_results.append(
                    {
                        "stage": stage_name,
                        "status": "FAILED",
                        "duration_seconds": round(
                            time.time() - stage_start,
                            2,
                        ),
                    }
                )

                raise

            except KeyboardInterrupt:
                stage_results.append(
                    {
                        "stage": stage_name,
                        "status": "INTERRUPTED",
                        "duration_seconds": round(
                            time.time() - stage_start,
                            2,
                        ),
                    }
                )

                raise

        status = "SUCCESS"

    except subprocess.CalledProcessError as error:
        status = "FAILED"
        failed_stage = stage_name
        exit_code = error.returncode
        error_message = (
            f"Etapa {stage_name} retornou "
            f"exit code {error.returncode}."
        )

        print(
            "\nPipeline incremental interrompido. "
            f"Exit code: {error.returncode}",
            file=sys.stderr,
        )

    except KeyboardInterrupt:
        status = "INTERRUPTED"
        failed_stage = stage_name
        exit_code = 130
        error_message = (
            "Pipeline interrompido pelo usuário."
        )

        print(
            "\nPipeline incremental interrompido "
            "pelo usuário."
        )

    finally:
        elapsed = time.time() - pipeline_start
        finished_at = utc_now()

        run_record = {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "duration_seconds": round(
                elapsed,
                2,
            ),
            "from_stage": args.from_stage,
            "selected_stages": selected_stages,
            "failed_stage": failed_stage,
            "exit_code": exit_code,
            "error_message": error_message,
            "stages": stage_results,
            "control_snapshot": (
                build_control_snapshot()
            ),
        }

        append_pipeline_run(
            run_record
        )

        print(
            "\nAuditoria registrada em: "
            "data/control/pipeline_runs.json"
        )
        print(f"Run ID: {run_id}")
        print(f"Status: {status}")

    if status != "SUCCESS":
        sys.exit(exit_code)

    print(
        f"\n{'=' * 60}\n"
        "PIPELINE INCREMENTAL CONCLUÍDO "
        "COM SUCESSO\n"
        f"Tempo total: {elapsed:.2f}s\n"
        f"{'=' * 60}"
    )


if __name__ == "__main__":
    main()
