import argparse
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1.0"
RESULT_PREFIX = "BENCHMARK_RESULT="

DEFAULT_OUTPUT = Path(
    "data/control/performance_benchmark.json"
)
DEFAULT_HISTORY = Path(
    "data/control/performance_benchmark_history.json"
)
DEFAULT_MARKDOWN = Path(
    "docs/performance-benchmark.md"
)

PROFILE_CONFIGS = {
    "baseline": {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.shuffle.partitions": "200",
    },
    "optimized": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.shuffle.partitions": "8",
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def nearest_rank_percentile(
    values: list[float],
    percentile: float,
) -> float:
    if not values:
        raise ValueError(
            "Não é possível calcular percentil sem amostras."
        )

    if not 0 < percentile <= 1:
        raise ValueError(
            "O percentil deve estar no intervalo (0, 1]."
        )

    ordered = sorted(values)
    index = math.ceil(percentile * len(ordered)) - 1
    return float(ordered[index])


def summarize_values(
    values: list[float],
) -> dict[str, float]:
    if not values:
        raise ValueError(
            "O benchmark não produziu amostras."
        )

    return {
        "minimum": round(min(values), 4),
        "median": round(statistics.median(values), 4),
        "p95": round(
            nearest_rank_percentile(values, 0.95),
            4,
        ),
        "mean": round(statistics.mean(values), 4),
        "maximum": round(max(values), 4),
        "stdev": round(
            statistics.pstdev(values),
            4,
        ),
    }


def summarize_profile(
    worker_result: dict[str, Any],
) -> dict[str, Any]:
    samples = worker_result["samples"]

    total_seconds = [
        float(sample["total_seconds"])
        for sample in samples
    ]
    throughput = [
        float(sample["rows_per_second"])
        for sample in samples
    ]

    stage_names = sorted(
        samples[0]["stages_seconds"]
    )

    stages = {
        stage_name: summarize_values(
            [
                float(
                    sample["stages_seconds"][stage_name]
                )
                for sample in samples
            ]
        )
        for stage_name in stage_names
    }

    fingerprints = [
        sample["fingerprint"]
        for sample in samples
    ]

    return {
        "profile": worker_result["profile"],
        "spark_config": worker_result["spark_config"],
        "spark_version": worker_result["spark_version"],
        "samples": samples,
        "total_seconds": summarize_values(total_seconds),
        "rows_per_second": summarize_values(throughput),
        "stages_seconds": stages,
        "stable_within_profile": all(
            fingerprint == fingerprints[0]
            for fingerprint in fingerprints
        ),
        "fingerprint": fingerprints[0],
    }


def compare_profiles(
    baseline: dict[str, Any],
    optimized: dict[str, Any],
    minimum_improvement_pct: float,
) -> dict[str, Any]:
    baseline_seconds = baseline[
        "total_seconds"
    ]["median"]
    optimized_seconds = optimized[
        "total_seconds"
    ]["median"]

    if baseline_seconds <= 0 or optimized_seconds <= 0:
        raise ValueError(
            "As medianas do benchmark devem ser positivas."
        )

    improvement_pct = (
        (baseline_seconds - optimized_seconds)
        / baseline_seconds
        * 100
    )

    fingerprints_match = (
        baseline["fingerprint"]
        == optimized["fingerprint"]
    )
    stable = (
        baseline["stable_within_profile"]
        and optimized["stable_within_profile"]
    )
    correctness_status = (
        "MATCH" if fingerprints_match and stable
        else "MISMATCH"
    )

    if correctness_status != "MATCH":
        recommendation = "REJECT_OPTIMIZATION"
    elif improvement_pct >= minimum_improvement_pct:
        recommendation = "ADOPT_OPTIMIZED_PROFILE"
    else:
        recommendation = "KEEP_BASELINE_AND_RETEST"

    return {
        "correctness_status": correctness_status,
        "minimum_improvement_pct": round(
            minimum_improvement_pct,
            2,
        ),
        "median_improvement_pct": round(
            improvement_pct,
            2,
        ),
        "speedup": round(
            baseline_seconds / optimized_seconds,
            3,
        ),
        "recommendation": recommendation,
    }


def parse_worker_result(stdout: str) -> dict[str, Any]:
    payloads = [
        line[len(RESULT_PREFIX):]
        for line in stdout.splitlines()
        if line.startswith(RESULT_PREFIX)
    ]

    if len(payloads) != 1:
        raise RuntimeError(
            "A execução worker deve publicar exatamente "
            "um resultado estruturado."
        )

    return json.loads(payloads[0])


def build_worker_command(
    script_path: Path,
    profile: str,
    rows: int,
    runs: int,
    warmups: int,
    master: str,
) -> list[str]:
    command = [
        "/opt/spark/bin/spark-submit",
        "--master",
        master,
    ]

    for key, value in PROFILE_CONFIGS[profile].items():
        command.extend(
            ["--conf", f"{key}={value}"]
        )

    command.extend(
        [
            str(script_path),
            "--worker",
            "--profile",
            profile,
            "--rows",
            str(rows),
            "--runs",
            str(runs),
            "--warmups",
            str(warmups),
        ]
    )

    return command


def run_worker(
    command: list[str],
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )

    if completed.returncode != 0:
        raise RuntimeError(
            "O worker do benchmark falhou com exit code "
            f"{completed.returncode}.\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}"
        )

    return parse_worker_result(completed.stdout)


def write_json(
    path: Path,
    payload: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )


def append_history(
    path: Path,
    benchmark: dict[str, Any],
) -> None:
    if path.exists():
        history = json.loads(
            path.read_text(encoding="utf-8")
        )
    else:
        history = {
            "schema_version": SCHEMA_VERSION,
            "benchmarks": [],
        }

    history["benchmarks"].append(benchmark)
    write_json(path, history)


def format_seconds(value: float) -> str:
    return f"{value:.2f}s"


def format_number(value: float) -> str:
    return f"{value:,.0f}".replace(",", ".")


def render_markdown(
    benchmark: dict[str, Any],
) -> str:
    baseline = benchmark["profiles"]["baseline"]
    optimized = benchmark["profiles"]["optimized"]
    comparison = benchmark["comparison"]

    recommendation_labels = {
        "ADOPT_OPTIMIZED_PROFILE": (
            "adotar o perfil otimizado"
        ),
        "KEEP_BASELINE_AND_RETEST": (
            "manter o perfil atual e repetir a medição"
        ),
        "REJECT_OPTIMIZATION": (
            "rejeitar a otimização por divergência funcional"
        ),
    }

    lines = [
        "# Benchmark de desempenho do pipeline",
        "",
        (
            "Este relatório compara duas configurações Spark sobre "
            "a mesma carga sintética e determinística. O objetivo é "
            "medir antes de alterar a configuração de produção."
        ),
        "",
        "## Escopo",
        "",
        f"- Registros de entrada: {format_number(benchmark['rows'])}",
        f"- Execuções medidas por perfil: {benchmark['runs']}",
        f"- Aquecimentos por perfil: {benchmark['warmups']}",
        f"- Spark: {baseline['spark_version']}",
        f"- Master: `{benchmark['master']}`",
        "",
        "## Resultado consolidado",
        "",
        "| Perfil | Mediana | P95 | Throughput mediano |",
        "| --- | ---: | ---: | ---: |",
        (
            "| Baseline | "
            f"{format_seconds(baseline['total_seconds']['median'])} | "
            f"{format_seconds(baseline['total_seconds']['p95'])} | "
            f"{format_number(baseline['rows_per_second']['median'])} reg/s |"
        ),
        (
            "| Otimizado | "
            f"{format_seconds(optimized['total_seconds']['median'])} | "
            f"{format_seconds(optimized['total_seconds']['p95'])} | "
            f"{format_number(optimized['rows_per_second']['median'])} reg/s |"
        ),
        "",
        "## Comparação por etapa",
        "",
        "| Etapa | Baseline | Otimizado |",
        "| --- | ---: | ---: |",
    ]

    for stage_name in sorted(baseline["stages_seconds"]):
        lines.append(
            f"| `{stage_name}` | "
            f"{format_seconds(baseline['stages_seconds'][stage_name]['median'])} | "
            f"{format_seconds(optimized['stages_seconds'][stage_name]['median'])} |"
        )

    lines.extend(
        [
            "",
            "## Decisão",
            "",
            (
                "- Equivalência dos resultados: "
                f"**{comparison['correctness_status']}**"
            ),
            (
                "- Variação da mediana: "
                f"**{comparison['median_improvement_pct']:.2f}%**"
            ),
            f"- Speedup: **{comparison['speedup']:.3f}x**",
            (
                "- Recomendação: **"
                + recommendation_labels[
                    comparison["recommendation"]
                ]
                + "**"
            ),
            "",
            "## Configurações comparadas",
            "",
            "### Baseline",
            "",
        ]
    )

    lines.extend(
        f"- `{key}={value}`"
        for key, value in baseline["spark_config"].items()
    )
    lines.extend(["", "### Otimizado", ""])
    lines.extend(
        f"- `{key}={value}`"
        for key, value in optimized["spark_config"].items()
    )
    lines.extend(
        [
            "",
            "## Limitações",
            "",
            (
                "A carga é sintética, não contém dados acadêmicos e "
                "serve para comparação controlada. Tempos absolutos "
                "dependem de CPU, memória, Docker e processos concorrentes."
            ),
            (
                "Uma configuração só deve ser aplicada ao pipeline "
                "principal quando mantiver os resultados equivalentes e "
                "apresentar ganho repetível no mesmo ambiente."
            ),
            "",
        ]
    )

    return "\n".join(lines)


def build_synthetic_raw(
    spark,
    row_count: int,
):
    from pyspark.sql import functions as F

    if row_count < 100:
        raise ValueError(
            "O benchmark exige pelo menos 100 registros."
        )

    if row_count > 9_000_000:
        raise ValueError(
            "O benchmark aceita no máximo 9.000.000 registros."
        )

    unique_incidents = row_count - max(
        1,
        row_count // 20,
    )

    base = (
        spark.range(row_count)
        .withColumn(
            "_incident_number",
            F.pmod(F.col("id"), F.lit(unique_incidents)),
        )
        .withColumn(
            "_priority_code",
            (F.pmod(F.col("id"), F.lit(5)) + 1).cast("int"),
        )
        .withColumn(
            "_duration",
            (F.pmod(F.col("id") * 97, F.lit(200_000)) + 600)
            .cast("long"),
        )
        .withColumn(
            "_opened_epoch",
            (
                F.lit(1_672_531_200)
                + F.pmod(
                    F.col("id") * 3_601,
                    F.lit(3 * 365 * 86_400),
                )
            ).cast("long"),
        )
    )

    priority_name = F.element_at(
        F.array(
            F.lit("Crítica"),
            F.lit("Alta"),
            F.lit("Média"),
            F.lit("Baixa"),
            F.lit("Muito Baixa"),
        ),
        F.col("_priority_code"),
    )

    opened_at = F.from_unixtime(
        F.col("_opened_epoch")
    )
    closed_at = F.from_unixtime(
        F.col("_opened_epoch") + F.col("_duration")
    )

    return base.select(
        F.format_string(
            "INC%07d",
            F.col("_incident_number") + 1,
        ).alias("Número"),
        F.concat_ws(
            " - ",
            F.col("_priority_code").cast("string"),
            priority_name,
        ).alias("Prioridade"),
        F.concat(
            F.lit("Produto"),
            F.pmod(
                F.col("id"),
                F.lit(12),
            ).cast("string"),
        ).alias("Produto"),
        F.concat(
            F.lit("Categoria"),
            F.pmod(
                F.col("id"),
                F.lit(18),
            ).cast("string"),
        ).alias("Categoria"),
        F.concat(
            F.lit("Subcategoria"),
            F.pmod(
                F.col("id"),
                F.lit(24),
            ).cast("string"),
        ).alias("Subcategoria"),
        F.concat(
            F.lit("Team"),
            F.format_string(
                "%02d",
                F.pmod(F.col("id"), F.lit(17)) + 1,
            ),
        ).alias("Grupo designado"),
        F.concat(
            F.lit("CI-"),
            F.pmod(
                F.col("id"),
                F.lit(30),
            ).cast("string"),
        ).alias("Item de configuração"),
        opened_at.alias("Aberto"),
        closed_at.alias("Resolvido"),
        closed_at.alias("Encerrado"),
        F.col("_duration").cast("string").alias("Duração"),
        F.lit("Resolvido").alias("Código de fechamento"),
        F.lit("Incidente sintético de benchmark").alias(
            "Descrição resumida"
        ),
        F.lit("Solução sintética").alias("Solução"),
        F.when(
            F.pmod(F.col("id"), F.lit(3)) == 0,
            F.lit("Monitoramento"),
        ).otherwise(F.lit("Manual")).alias("Aberto por"),
        F.lit(None).cast("string").alias("Incidente Pai"),
        F.lit("Encerrado").alias("Status"),
        F.when(
            F.col("_priority_code") <= 3,
            F.lit("SIM"),
        ).otherwise(F.lit("NAO")).alias("Entrou para KPI?"),
        F.when(
            (F.col("_priority_code") <= 3)
            & (F.col("_duration") > 14_400),
            F.lit("SIM"),
        ).otherwise(F.lit("NAO")).alias("KPI Violado?"),
        F.to_timestamp(opened_at).alias("_ingested_at"),
        F.lit("synthetic-benchmark.csv").alias("_source_file"),
    )


def dataframe_fingerprint(df) -> dict[str, int]:
    from pyspark.sql import functions as F

    row = (
        df.agg(
            F.count("*").alias("rows"),
            F.sum("duration_seconds").alias("duration_sum"),
            F.sum("priority_code").alias("priority_sum"),
        )
        .first()
    )

    return {
        "rows": int(row["rows"] or 0),
        "duration_sum": int(row["duration_sum"] or 0),
        "priority_sum": int(row["priority_sum"] or 0),
    }


def count_frames(
    frames: dict[str, Any],
) -> dict[str, int]:
    return {
        name: int(frame.count())
        for name, frame in frames.items()
    }


def execute_workload(
    spark,
    row_count: int,
    cache_shared_frames: bool,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    from src.forecast_gold import (
        create_forecast_history,
        create_forecast_summary,
    )
    from src.gold import (
        create_dashboard_summary,
        create_monthly_kpis,
        create_priority_summary,
        create_team_summary,
    )
    from src.operational_gold import (
        create_annual_ola_summary,
        create_daily_trends,
        create_operational_kpi_summary,
    )
    from src.recommendation_gold import (
        create_recommendations,
    )
    from src.risk_gold import create_risk_summary
    from src.silver import deduplicate, transform_records

    cached_frames = []
    stages = {}

    raw_df = build_synthetic_raw(
        spark,
        row_count,
    ).cache()
    cached_frames.append(raw_df)
    raw_df.count()

    total_start = time.perf_counter()

    try:
        stage_start = time.perf_counter()
        valid_df = (
            deduplicate(transform_records(raw_df))
            .filter(F.col("dq_status") == "VALID")
            .cache()
        )
        cached_frames.append(valid_df)
        fingerprint = dataframe_fingerprint(valid_df)
        stages["silver_transform_and_dedup"] = (
            time.perf_counter() - stage_start
        )

        stage_start = time.perf_counter()
        core_frames = {
            "monthly_kpis": create_monthly_kpis(valid_df),
            "priority_summary": create_priority_summary(valid_df),
            "team_summary": create_team_summary(valid_df),
            "dashboard_summary": create_dashboard_summary(valid_df),
        }
        output_counts = count_frames(core_frames)
        stages["core_gold"] = time.perf_counter() - stage_start

        stage_start = time.perf_counter()
        daily_df = create_daily_trends(valid_df)
        operational_df = create_operational_kpi_summary(valid_df)
        annual_df = create_annual_ola_summary(valid_df)
        risk_df = create_risk_summary(valid_df)
        forecast_history_df = create_forecast_history(valid_df)
        forecast_summary_df = create_forecast_summary(valid_df)

        shared_frames = [
            annual_df,
            risk_df,
            forecast_history_df,
            forecast_summary_df,
        ]

        if cache_shared_frames:
            for frame in shared_frames:
                frame.cache()
                cached_frames.append(frame)

        operational_frames = {
            "daily_trends": daily_df,
            "operational_kpi_summary": operational_df,
            "annual_ola_summary": annual_df,
            "risk_summary": risk_df,
            "forecast_history": forecast_history_df,
            "forecast_summary": forecast_summary_df,
        }
        output_counts.update(
            count_frames(operational_frames)
        )

        recommendations_df = create_recommendations(
            risk_df,
            annual_df,
            forecast_summary_df,
            forecast_history_df,
        )
        output_counts["recommendations"] = int(
            recommendations_df.count()
        )
        stages["operational_gold"] = (
            time.perf_counter() - stage_start
        )

        total_seconds = time.perf_counter() - total_start

        return {
            "total_seconds": round(total_seconds, 4),
            "rows_per_second": round(
                fingerprint["rows"] / total_seconds,
                4,
            ),
            "stages_seconds": {
                name: round(value, 4)
                for name, value in stages.items()
            },
            "fingerprint": {
                **fingerprint,
                "output_counts": output_counts,
            },
        }

    finally:
        for frame in reversed(cached_frames):
            frame.unpersist()
        spark.catalog.clearCache()


def run_worker_mode(args: argparse.Namespace) -> None:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(
            f"IT Incident Benchmark - {args.profile}"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for _ in range(args.warmups):
            execute_workload(
                spark,
                args.rows,
                cache_shared_frames=(
                    args.profile == "optimized"
                ),
            )

        samples = [
            execute_workload(
                spark,
                args.rows,
                cache_shared_frames=(
                    args.profile == "optimized"
                ),
            )
            for _ in range(args.runs)
        ]

        result = {
            "profile": args.profile,
            "spark_version": spark.version,
            "spark_config": PROFILE_CONFIGS[args.profile],
            "samples": samples,
        }

        print(
            RESULT_PREFIX
            + json.dumps(
                result,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

    finally:
        spark.stop()


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compara uma configuração Spark de referência com "
            "um perfil otimizado sobre uma carga determinística."
        )
    )
    parser.add_argument("--worker", action="store_true")
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILE_CONFIGS),
        default="baseline",
    )
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument(
        "--minimum-improvement-pct",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )
    parser.add_argument(
        "--history",
        type=Path,
        default=DEFAULT_HISTORY,
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=DEFAULT_MARKDOWN,
    )
    return parser


def validate_controller_args(
    args: argparse.Namespace,
) -> None:
    if not 100 <= args.rows <= 9_000_000:
        raise ValueError(
            "--rows deve estar entre 100 e 9.000.000."
        )
    if args.runs < 1:
        raise ValueError("--runs deve ser pelo menos 1.")
    if args.warmups < 0:
        raise ValueError("--warmups não pode ser negativo.")
    if args.minimum_improvement_pct < 0:
        raise ValueError(
            "--minimum-improvement-pct não pode ser negativo."
        )


def run_controller_mode(
    args: argparse.Namespace,
) -> dict[str, Any]:
    validate_controller_args(args)
    script_path = Path(__file__).resolve()

    profiles = {}

    for profile in PROFILE_CONFIGS:
        print(
            f"Executando perfil {profile}: "
            f"{args.rows} registros, "
            f"{args.runs} medição(ões).",
            flush=True,
        )
        command = build_worker_command(
            script_path,
            profile,
            args.rows,
            args.runs,
            args.warmups,
            args.master,
        )
        profiles[profile] = summarize_profile(
            run_worker(command)
        )

    comparison = compare_profiles(
        profiles["baseline"],
        profiles["optimized"],
        args.minimum_improvement_pct,
    )

    benchmark = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "rows": args.rows,
        "runs": args.runs,
        "warmups": args.warmups,
        "master": args.master,
        "environment": {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "profiles": profiles,
        "comparison": comparison,
    }

    write_json(args.output, benchmark)
    append_history(args.history, benchmark)

    args.markdown.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    args.markdown.write_text(
        render_markdown(benchmark),
        encoding="utf-8",
    )

    print(f"Resultado JSON: {args.output}")
    print(f"Relatório Markdown: {args.markdown}")
    print(
        "Equivalência funcional: "
        f"{comparison['correctness_status']}"
    )
    print(
        "Variação da mediana: "
        f"{comparison['median_improvement_pct']:.2f}%"
    )
    print(
        "Recomendação: "
        f"{comparison['recommendation']}"
    )

    if comparison["correctness_status"] != "MATCH":
        raise RuntimeError(
            "Os perfis produziram resultados divergentes."
        )

    return benchmark


def main() -> None:
    args = create_parser().parse_args()

    if args.worker:
        run_worker_mode(args)
    else:
        run_controller_mode(args)


if __name__ == "__main__":
    main()
