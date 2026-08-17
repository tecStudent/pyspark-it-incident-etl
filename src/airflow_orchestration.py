import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


STAGE_SCRIPTS = {
    "bronze": "src/incremental_bronze.py",
    "silver": "src/incremental_silver.py",
    "gold": "src/incremental_gold.py",
    "reconciliation": "src/pipeline_reconciliation.py",
}

DEFAULT_PROJECT_ROOT = Path(
    "/opt/airflow/project"
)

DEFAULT_AUDIT_PATH = Path(
    "data/control/airflow_pipeline_runs.json"
)


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def resolve_project_root(
    explicit_root: str | Path | None = None,
) -> Path:
    configured_root = (
        explicit_root
        or os.getenv("AIRFLOW_PROJECT_ROOT")
        or DEFAULT_PROJECT_ROOT
    )

    return Path(configured_root).resolve()


def inspect_landing(
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    root = resolve_project_root(project_root)
    landing_dir = root / "data" / "landing"
    files = (
        sorted(landing_dir.glob("*.csv"))
        if landing_dir.exists()
        else []
    )

    return {
        "status": "READY",
        "landing_path": str(landing_dir),
        "csv_count": len(files),
        "total_bytes": sum(
            file_path.stat().st_size
            for file_path in files
        ),
        "files": [
            file_path.name
            for file_path in files
        ],
        "checked_at": utc_now(),
    }


def resolve_spark_submit(
    explicit_path: str | Path | None = None,
) -> str:
    configured_path = (
        explicit_path
        or os.getenv("SPARK_SUBMIT_BIN")
    )

    if configured_path:
        return str(configured_path)

    path_command = shutil.which(
        "spark-submit"
    )

    if path_command:
        return path_command

    try:
        import pyspark
    except ImportError as error:
        raise FileNotFoundError(
            "spark-submit não foi encontrado. "
            "Instale o PySpark no runtime do Airflow."
        ) from error

    bundled_command = (
        Path(pyspark.__file__).parent
        / "bin"
        / "spark-submit"
    )

    if not bundled_command.exists():
        raise FileNotFoundError(
            "O PySpark está instalado, mas o "
            "spark-submit não foi localizado."
        )

    return str(bundled_command)


def build_stage_command(
    stage_name: str,
    project_root: str | Path | None = None,
    spark_submit: str | Path | None = None,
    spark_master: str | None = None,
) -> list[str]:
    if stage_name not in STAGE_SCRIPTS:
        raise ValueError(
            f"Etapa Airflow desconhecida: "
            f"{stage_name}"
        )

    root = resolve_project_root(project_root)
    script_path = (
        root / STAGE_SCRIPTS[stage_name]
    ).resolve()

    try:
        script_path.relative_to(root)
    except ValueError as error:
        raise ValueError(
            "O script da etapa está fora "
            "do diretório do projeto."
        ) from error

    if not script_path.is_file():
        raise FileNotFoundError(
            f"Script da etapa não encontrado: "
            f"{script_path}"
        )

    return [
        resolve_spark_submit(spark_submit),
        "--master",
        (
            spark_master
            or os.getenv("SPARK_MASTER")
            or "local[2]"
        ),
        str(script_path),
    ]


def run_stage(
    stage_name: str,
    project_root: str | Path | None = None,
    spark_submit: str | Path | None = None,
    spark_master: str | None = None,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    root = resolve_project_root(project_root)
    command = build_stage_command(
        stage_name=stage_name,
        project_root=root,
        spark_submit=spark_submit,
        spark_master=spark_master,
    )

    run_command = runner or subprocess.run
    started_at = utc_now()
    started_clock = time.monotonic()

    print(
        f"Executando etapa Airflow: "
        f"{stage_name}",
        flush=True,
    )
    print(
        f"Comando: {' '.join(command)}",
        flush=True,
    )

    environment = os.environ.copy()
    current_pythonpath = environment.get(
        "PYTHONPATH",
        "",
    )
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(root),
            current_pythonpath,
        )
        if value
    )

    run_command(
        command,
        cwd=str(root),
        env=environment,
        check=True,
    )

    duration = (
        time.monotonic()
        - started_clock
    )

    result = {
        "stage": stage_name,
        "status": "SUCCESS",
        "started_at": started_at,
        "finished_at": utc_now(),
        "duration_seconds": round(
            duration,
            2,
        ),
        "command": command,
    }

    print(
        f"Etapa {stage_name} concluída "
        f"em {duration:.2f}s.",
        flush=True,
    )

    return result


def append_airflow_run(
    run_record: dict[str, Any],
    project_root: str | Path | None = None,
    audit_path: str | Path = DEFAULT_AUDIT_PATH,
) -> Path:
    root = resolve_project_root(project_root)
    destination = (
        root / Path(audit_path)
    ).resolve()

    try:
        destination.relative_to(root)
    except ValueError as error:
        raise ValueError(
            "O arquivo de auditoria está fora "
            "do diretório do projeto."
        ) from error

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if destination.exists():
        with destination.open(
            "r",
            encoding="utf-8",
        ) as source:
            document = json.load(source)
    else:
        document = {
            "version": 1,
            "runs": [],
        }

    if not isinstance(
        document.get("runs"),
        list,
    ):
        raise ValueError(
            "Arquivo de auditoria Airflow "
            "inválido: campo 'runs' ausente."
        )

    document["runs"].append(
        run_record
    )

    temporary_path = (
        destination.with_suffix(".tmp")
    )

    with temporary_path.open(
        "w",
        encoding="utf-8",
    ) as target:
        json.dump(
            document,
            target,
            ensure_ascii=False,
            indent=2,
        )
        target.write("\n")

    os.replace(
        temporary_path,
        destination,
    )

    return destination
