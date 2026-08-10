import argparse
import subprocess
import sys
import time


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
}


def run_stage(
    stage_name: str,
    command: list[str],
) -> None:
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

    pipeline_start = time.time()

    try:
        for stage_name in selected_stages:
            run_stage(
                stage_name,
                STAGES[stage_name],
            )

    except subprocess.CalledProcessError as error:
        print(
            "\nPipeline incremental interrompido. "
            f"Exit code: {error.returncode}",
            file=sys.stderr,
        )

        sys.exit(error.returncode)

    except KeyboardInterrupt:
        print(
            "\nPipeline incremental interrompido "
            "pelo usuário."
        )

        sys.exit(130)

    elapsed = time.time() - pipeline_start

    print(
        f"\n{'=' * 60}\n"
        "PIPELINE INCREMENTAL CONCLUÍDO "
        "COM SUCESSO\n"
        f"Tempo total: {elapsed:.2f}s\n"
        f"{'=' * 60}"
    )


if __name__ == "__main__":
    main()