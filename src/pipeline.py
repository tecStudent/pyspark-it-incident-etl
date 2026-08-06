import argparse
import subprocess
import sys
import time


STAGES = {
    "extract": [
        "python3",
        "src/extract_xlsx.py",
    ],
    "bronze": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[4]",
        "src/bronze.py",
    ],
    "silver": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[4]",
        "src/silver.py",
    ],
    "gold": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[4]",
        "src/gold.py",
    ],
    "dashboard": [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "src/export_dashboard.py",
    ],
}


def run_stage(
    stage_name: str,
    command: list[str],
) -> None:
    print(
        f"\n{'=' * 60}\n"
        f"Executando etapa: {stage_name.upper()}\n"
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
        f"\nEtapa {stage_name.upper()} concluída "
        f"em {elapsed:.2f}s",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Executa o pipeline de dados de incidentes."
    )

    parser.add_argument(
        "--from-stage",
        choices=list(STAGES.keys()),
        default="extract",
        help="Etapa a partir da qual o pipeline será executado.",
    )

    args = parser.parse_args()

    stage_names = list(STAGES.keys())

    start_index = stage_names.index(
        args.from_stage
    )

    selected_stages = stage_names[start_index:]

    pipeline_start = time.time()

    try:
        for stage_name in selected_stages:
            run_stage(
                stage_name,
                STAGES[stage_name],
            )

    except subprocess.CalledProcessError as error:
        print(
            f"\nPipeline interrompido por erro "
            f"na etapa atual. Exit code: {error.returncode}",
            file=sys.stderr,
        )

        sys.exit(error.returncode)

    except KeyboardInterrupt:
        print(
            "\nPipeline interrompido pelo usuário."
        )

        sys.exit(130)

    elapsed = time.time() - pipeline_start

    print(
        f"\n{'=' * 60}\n"
        "PIPELINE CONCLUÍDO COM SUCESSO\n"
        f"Tempo total: {elapsed:.2f}s\n"
        f"{'=' * 60}"
    )


if __name__ == "__main__":
    main()