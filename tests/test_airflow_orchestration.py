import ast
import json
import subprocess
from pathlib import Path

import pytest

from src import airflow_orchestration


PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)


def create_stage_scripts(
    root: Path,
) -> None:
    for relative_path in (
        airflow_orchestration
        .STAGE_SCRIPTS
        .values()
    ):
        script_path = root / relative_path
        script_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        script_path.write_text(
            "print('ok')\n",
            encoding="utf-8",
        )


def test_stage_scripts_match_incremental_pipeline():
    assert airflow_orchestration.STAGE_SCRIPTS == {
        "bronze": (
            "src/incremental_bronze.py"
        ),
        "silver": (
            "src/incremental_silver.py"
        ),
        "gold": (
            "src/incremental_gold.py"
        ),
        "reconciliation": (
            "src/pipeline_reconciliation.py"
        ),
    }


def test_resolve_project_root_uses_explicit_path(
    tmp_path,
):
    assert (
        airflow_orchestration
        .resolve_project_root(tmp_path)
        == tmp_path.resolve()
    )


def test_resolve_project_root_uses_environment(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "AIRFLOW_PROJECT_ROOT",
        str(tmp_path),
    )

    assert (
        airflow_orchestration
        .resolve_project_root()
        == tmp_path.resolve()
    )


def test_inspect_landing_accepts_missing_directory(
    tmp_path,
):
    result = (
        airflow_orchestration
        .inspect_landing(tmp_path)
    )

    assert result["status"] == "READY"
    assert result["csv_count"] == 0
    assert result["total_bytes"] == 0
    assert result["files"] == []


def test_inspect_landing_lists_only_csv_files(
    tmp_path,
):
    landing = tmp_path / "data" / "landing"
    landing.mkdir(
        parents=True
    )
    (landing / "b.csv").write_text(
        "123",
        encoding="utf-8",
    )
    (landing / "a.csv").write_text(
        "12",
        encoding="utf-8",
    )
    (landing / "ignore.txt").write_text(
        "ignored",
        encoding="utf-8",
    )

    result = (
        airflow_orchestration
        .inspect_landing(tmp_path)
    )

    assert result["csv_count"] == 2
    assert result["total_bytes"] == 5
    assert result["files"] == [
        "a.csv",
        "b.csv",
    ]


def test_resolve_spark_submit_uses_explicit_path():
    result = (
        airflow_orchestration
        .resolve_spark_submit(
            "/spark/bin/spark-submit"
        )
    )

    assert result == (
        "/spark/bin/spark-submit"
    )


def test_resolve_spark_submit_uses_environment(
    monkeypatch,
):
    monkeypatch.setenv(
        "SPARK_SUBMIT_BIN",
        "/custom/spark-submit",
    )

    assert (
        airflow_orchestration
        .resolve_spark_submit()
        == "/custom/spark-submit"
    )


def test_build_stage_command(
    tmp_path,
):
    create_stage_scripts(tmp_path)

    command = (
        airflow_orchestration
        .build_stage_command(
            stage_name="silver",
            project_root=tmp_path,
            spark_submit="spark-submit-test",
            spark_master="local[1]",
        )
    )

    assert command[:3] == [
        "spark-submit-test",
        "--master",
        "local[1]",
    ]
    assert command[3].endswith(
        "src/incremental_silver.py"
    )


def test_build_stage_command_rejects_unknown_stage(
    tmp_path,
):
    with pytest.raises(
        ValueError,
        match="desconhecida",
    ):
        (
            airflow_orchestration
            .build_stage_command(
                stage_name="unknown",
                project_root=tmp_path,
                spark_submit="spark-submit",
            )
        )


def test_build_stage_command_requires_script(
    tmp_path,
):
    with pytest.raises(
        FileNotFoundError,
        match="não encontrado",
    ):
        (
            airflow_orchestration
            .build_stage_command(
                stage_name="gold",
                project_root=tmp_path,
                spark_submit="spark-submit",
            )
        )


def test_run_stage_invokes_checked_subprocess(
    tmp_path,
):
    create_stage_scripts(tmp_path)
    calls = []

    def runner(
        command,
        **kwargs,
    ):
        calls.append(
            (command, kwargs)
        )

    result = (
        airflow_orchestration.run_stage(
            stage_name="bronze",
            project_root=tmp_path,
            spark_submit="spark-submit-test",
            spark_master="local[1]",
            runner=runner,
        )
    )

    assert result["status"] == "SUCCESS"
    assert result["stage"] == "bronze"
    assert calls[0][1]["check"] is True
    assert calls[0][1]["cwd"] == str(
        tmp_path.resolve()
    )
    assert (
        str(tmp_path.resolve())
        in calls[0][1]["env"]["PYTHONPATH"]
    )


def test_run_stage_propagates_subprocess_failure(
    tmp_path,
):
    create_stage_scripts(tmp_path)

    def failing_runner(
        command,
        **kwargs,
    ):
        raise subprocess.CalledProcessError(
            7,
            command,
        )

    with pytest.raises(
        subprocess.CalledProcessError,
    ):
        airflow_orchestration.run_stage(
            stage_name="gold",
            project_root=tmp_path,
            spark_submit="spark-submit-test",
            runner=failing_runner,
        )


def test_append_airflow_run_creates_audit(
    tmp_path,
):
    destination = (
        airflow_orchestration
        .append_airflow_run(
            {"run_id": "run-1"},
            project_root=tmp_path,
        )
    )

    document = json.loads(
        destination.read_text(
            encoding="utf-8",
        )
    )

    assert document["version"] == 1
    assert document["runs"] == [
        {"run_id": "run-1"}
    ]


def test_append_airflow_run_preserves_history(
    tmp_path,
):
    for run_id in (
        "run-1",
        "run-2",
    ):
        (
            airflow_orchestration
            .append_airflow_run(
                {"run_id": run_id},
                project_root=tmp_path,
            )
        )

    destination = (
        tmp_path
        / airflow_orchestration
        .DEFAULT_AUDIT_PATH
    )
    document = json.loads(
        destination.read_text(
            encoding="utf-8",
        )
    )

    assert [
        item["run_id"]
        for item in document["runs"]
    ] == [
        "run-1",
        "run-2",
    ]


def test_append_airflow_run_rejects_invalid_audit(
    tmp_path,
):
    destination = (
        tmp_path
        / airflow_orchestration
        .DEFAULT_AUDIT_PATH
    )
    destination.parent.mkdir(
        parents=True
    )
    destination.write_text(
        '{"version": 1}',
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="inválido",
    ):
        (
            airflow_orchestration
            .append_airflow_run(
                {"run_id": "run-1"},
                project_root=tmp_path,
            )
        )


def test_append_airflow_run_rejects_outside_path(
    tmp_path,
):
    with pytest.raises(
        ValueError,
        match="fora",
    ):
        (
            airflow_orchestration
            .append_airflow_run(
                {"run_id": "run-1"},
                project_root=tmp_path,
                audit_path=(
                    tmp_path.parent
                    / "outside.json"
                ),
            )
        )


def test_dag_has_valid_python_syntax():
    dag_path = (
        PROJECT_ROOT
        / "dags"
        / "it_incident_incremental_pipeline.py"
    )

    ast.parse(
        dag_path.read_text(
            encoding="utf-8",
        )
    )


def test_dag_uses_airflow_three_public_sdk():
    dag_text = (
        PROJECT_ROOT
        / "dags"
        / "it_incident_incremental_pipeline.py"
    ).read_text(
        encoding="utf-8",
    )

    assert (
        "from airflow.sdk import "
        "dag, get_current_context, task"
        in dag_text
    )
    assert (
        "airflow.operators"
        not in dag_text
    )


def test_dag_defines_expected_tasks():
    dag_text = (
        PROJECT_ROOT
        / "dags"
        / "it_incident_incremental_pipeline.py"
    ).read_text(
        encoding="utf-8",
    )

    for task_id in (
        "check_landing",
        "bronze",
        "silver",
        "gold",
        "reconciliation",
        "execution_summary",
    ):
        assert task_id in dag_text


def test_dag_protects_local_controls():
    dag_text = (
        PROJECT_ROOT
        / "dags"
        / "it_incident_incremental_pipeline.py"
    ).read_text(
        encoding="utf-8",
    )

    assert "max_active_runs=1" in dag_text
    assert "catchup=False" in dag_text
    assert '"retries": 1' in dag_text


def test_compose_defines_required_services():
    compose_text = (
        PROJECT_ROOT
        / "docker-compose.airflow.yml"
    ).read_text(
        encoding="utf-8",
    )

    for service in (
        "postgres:",
        "airflow-api-server:",
        "airflow-scheduler:",
        "airflow-dag-processor:",
        "airflow-init:",
    ):
        assert service in compose_text


def test_compose_uses_local_executor():
    compose_text = (
        PROJECT_ROOT
        / "docker-compose.airflow.yml"
    ).read_text(
        encoding="utf-8",
    )

    assert (
        "AIRFLOW__CORE__EXECUTOR: "
        "LocalExecutor"
        in compose_text
    )
    assert (
        "postgresql+psycopg2"
        in compose_text
    )


def test_compose_does_not_mount_docker_socket():
    compose_text = (
        PROJECT_ROOT
        / "docker-compose.airflow.yml"
    ).read_text(
        encoding="utf-8",
    )

    assert (
        "/var/run/docker.sock"
        not in compose_text
    )


def test_airflow_dockerfile_pins_runtime():
    dockerfile = (
        PROJECT_ROOT
        / "Dockerfile.airflow"
    ).read_text(
        encoding="utf-8",
    )
    requirements = (
        PROJECT_ROOT
        / "requirements-airflow.txt"
    ).read_text(
        encoding="utf-8",
    )

    assert (
        "apache/airflow:"
        in dockerfile
    )
    assert (
        "AIRFLOW_VERSION=3.3.1"
        in dockerfile
    )
    assert (
        "openjdk-17-jre-headless"
        in dockerfile
    )
    assert (
        "pyspark==4.1.2"
        in requirements
    )


def test_example_environment_is_manual_by_default():
    environment = (
        PROJECT_ROOT
        / ".env.airflow.example"
    ).read_text(
        encoding="utf-8",
    )

    assert (
        "AIRFLOW_PIPELINE_SCHEDULE=\n"
        in environment
    )
    assert (
        "AIRFLOW_PROJECT_ROOT="
        "/opt/airflow/project"
        in environment
    )


def test_documentation_marks_environment_as_local():
    documentation = (
        PROJECT_ROOT
        / "docs"
        / "airflow-orchestration.md"
    ).read_text(
        encoding="utf-8",
    )

    assert (
        "demonstração local"
        in documentation
    )
    assert (
        "não para produção distribuída"
        in documentation
    )
    assert (
        "down --volumes --remove-orphans"
        in documentation
    )
