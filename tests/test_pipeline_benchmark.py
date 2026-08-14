import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import pipeline_benchmark as benchmark


FINGERPRINT = {
    "rows": 950,
    "duration_sum": 123_456,
    "priority_sum": 2_850,
    "output_counts": {
        "monthly_kpis": 36,
        "priority_summary": 5,
    },
}


def sample(
    seconds: float,
    fingerprint=None,
):
    return {
        "total_seconds": seconds,
        "rows_per_second": 950 / seconds,
        "stages_seconds": {
            "silver_transform_and_dedup": seconds * 0.4,
            "core_gold": seconds * 0.2,
            "operational_gold": seconds * 0.4,
        },
        "fingerprint": fingerprint or FINGERPRINT,
    }


def worker_result(
    profile: str,
    seconds=(10.0, 9.0, 11.0),
    fingerprints=None,
):
    if fingerprints is None:
        fingerprints = [FINGERPRINT] * len(seconds)

    return {
        "profile": profile,
        "spark_version": "4.1.2",
        "spark_config": benchmark.PROFILE_CONFIGS[profile],
        "samples": [
            sample(value, fingerprint)
            for value, fingerprint in zip(
                seconds,
                fingerprints,
            )
        ],
    }


def summarized_pair():
    baseline = benchmark.summarize_profile(
        worker_result("baseline")
    )
    optimized = benchmark.summarize_profile(
        worker_result(
            "optimized",
            seconds=(8.0, 7.0, 9.0),
        )
    )
    return baseline, optimized


def test_nearest_rank_percentile():
    assert benchmark.nearest_rank_percentile(
        [3.0, 1.0, 2.0, 4.0],
        0.50,
    ) == 2.0
    assert benchmark.nearest_rank_percentile(
        [3.0, 1.0, 2.0, 4.0],
        0.95,
    ) == 4.0


def test_nearest_rank_percentile_rejects_empty_values():
    with pytest.raises(ValueError, match="sem amostras"):
        benchmark.nearest_rank_percentile([], 0.95)


def test_nearest_rank_percentile_rejects_invalid_percentile():
    with pytest.raises(ValueError, match="intervalo"):
        benchmark.nearest_rank_percentile([1.0], 0.0)


def test_summarize_values():
    result = benchmark.summarize_values(
        [8.0, 10.0, 12.0]
    )

    assert result == {
        "minimum": 8.0,
        "median": 10.0,
        "p95": 12.0,
        "mean": 10.0,
        "maximum": 12.0,
        "stdev": 1.633,
    }


def test_summarize_values_rejects_empty_values():
    with pytest.raises(ValueError, match="não produziu"):
        benchmark.summarize_values([])


def test_summarize_profile():
    result = benchmark.summarize_profile(
        worker_result("baseline")
    )

    assert result["profile"] == "baseline"
    assert result["total_seconds"]["median"] == 10.0
    assert result["total_seconds"]["p95"] == 11.0
    assert result["stable_within_profile"] is True
    assert result["fingerprint"] == FINGERPRINT
    assert set(result["stages_seconds"]) == {
        "silver_transform_and_dedup",
        "core_gold",
        "operational_gold",
    }


def test_summarize_profile_detects_unstable_results():
    changed = {**FINGERPRINT, "rows": 949}
    result = benchmark.summarize_profile(
        worker_result(
            "baseline",
            seconds=(10.0, 11.0),
            fingerprints=[FINGERPRINT, changed],
        )
    )

    assert result["stable_within_profile"] is False


def test_compare_profiles_recommends_optimized_profile():
    baseline, optimized = summarized_pair()
    result = benchmark.compare_profiles(
        baseline,
        optimized,
        minimum_improvement_pct=5.0,
    )

    assert result["correctness_status"] == "MATCH"
    assert result["median_improvement_pct"] == 20.0
    assert result["speedup"] == 1.25
    assert result["recommendation"] == (
        "ADOPT_OPTIMIZED_PROFILE"
    )


def test_compare_profiles_keeps_baseline_for_small_gain():
    baseline = benchmark.summarize_profile(
        worker_result(
            "baseline",
            seconds=(10.0, 10.0),
        )
    )
    optimized = benchmark.summarize_profile(
        worker_result(
            "optimized",
            seconds=(9.8, 9.8),
        )
    )

    result = benchmark.compare_profiles(
        baseline,
        optimized,
        minimum_improvement_pct=5.0,
    )

    assert result["recommendation"] == (
        "KEEP_BASELINE_AND_RETEST"
    )


def test_compare_profiles_rejects_different_results():
    baseline, optimized = summarized_pair()
    optimized["fingerprint"] = {
        **FINGERPRINT,
        "rows": 900,
    }

    result = benchmark.compare_profiles(
        baseline,
        optimized,
        minimum_improvement_pct=5.0,
    )

    assert result["correctness_status"] == "MISMATCH"
    assert result["recommendation"] == "REJECT_OPTIMIZATION"


def test_compare_profiles_rejects_non_positive_median():
    baseline, optimized = summarized_pair()
    baseline["total_seconds"]["median"] = 0

    with pytest.raises(ValueError, match="positivas"):
        benchmark.compare_profiles(
            baseline,
            optimized,
            minimum_improvement_pct=5.0,
        )


def test_parse_worker_result():
    payload = worker_result("baseline", seconds=(1.0,))
    stdout = (
        "Spark log\n"
        + benchmark.RESULT_PREFIX
        + json.dumps(payload)
        + "\n"
    )

    assert benchmark.parse_worker_result(stdout) == payload


def test_parse_worker_result_requires_one_payload():
    with pytest.raises(RuntimeError, match="exatamente"):
        benchmark.parse_worker_result("sem resultado")


def test_build_worker_command():
    command = benchmark.build_worker_command(
        Path("src/pipeline_benchmark.py"),
        "optimized",
        rows=50_000,
        runs=3,
        warmups=1,
        master="local[2]",
    )

    assert command[:3] == [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
    ]
    assert "spark.sql.adaptive.enabled=true" in command
    assert "spark.sql.shuffle.partitions=8" in command
    assert command[-10:] == [
        "src/pipeline_benchmark.py",
        "--worker",
        "--profile",
        "optimized",
        "--rows",
        "50000",
        "--runs",
        "3",
        "--warmups",
        "1",
    ]


def test_run_worker_returns_structured_result(monkeypatch):
    payload = worker_result("baseline", seconds=(1.0,))

    monkeypatch.setattr(
        benchmark.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=(
                benchmark.RESULT_PREFIX
                + json.dumps(payload)
            ),
            stderr="",
        ),
    )

    assert benchmark.run_worker(["command"]) == payload


def test_run_worker_reports_subprocess_failure(monkeypatch):
    monkeypatch.setattr(
        benchmark.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=7,
            stdout="partial output",
            stderr="spark error",
        ),
    )

    with pytest.raises(RuntimeError, match="exit code 7"):
        benchmark.run_worker(["command"])


def test_write_json_creates_parent_directory(tmp_path):
    output = tmp_path / "nested" / "result.json"
    benchmark.write_json(output, {"status": "OK"})

    assert json.loads(output.read_text()) == {
        "status": "OK"
    }


def test_append_history_creates_and_appends(tmp_path):
    history_path = tmp_path / "history.json"

    benchmark.append_history(
        history_path,
        {"generated_at": "first"},
    )
    benchmark.append_history(
        history_path,
        {"generated_at": "second"},
    )

    history = json.loads(history_path.read_text())
    assert history["schema_version"] == "1.0"
    assert [
        item["generated_at"]
        for item in history["benchmarks"]
    ] == ["first", "second"]


def test_render_markdown():
    baseline, optimized = summarized_pair()
    payload = {
        "rows": 50_000,
        "runs": 3,
        "warmups": 1,
        "master": "local[2]",
        "profiles": {
            "baseline": baseline,
            "optimized": optimized,
        },
        "comparison": benchmark.compare_profiles(
            baseline,
            optimized,
            5.0,
        ),
    }

    markdown = benchmark.render_markdown(payload)

    assert "# Benchmark de desempenho" in markdown
    assert "50.000" in markdown
    assert "53" not in markdown
    assert "adotar o perfil otimizado" in markdown
    assert "spark.sql.shuffle.partitions=8" in markdown


def test_format_helpers():
    assert benchmark.format_seconds(1.236) == "1.24s"
    assert benchmark.format_number(50_000) == "50.000"


def test_validate_controller_args_accepts_valid_values():
    args = SimpleNamespace(
        rows=50_000,
        runs=3,
        warmups=1,
        minimum_improvement_pct=5.0,
    )

    benchmark.validate_controller_args(args)


def test_validate_controller_args_rejects_invalid_rows():
    args = SimpleNamespace(
        rows=99,
        runs=3,
        warmups=1,
        minimum_improvement_pct=5.0,
    )

    with pytest.raises(ValueError, match="--rows"):
        benchmark.validate_controller_args(args)


def test_validate_controller_args_rejects_invalid_runs():
    args = SimpleNamespace(
        rows=50_000,
        runs=0,
        warmups=1,
        minimum_improvement_pct=5.0,
    )

    with pytest.raises(ValueError, match="--runs"):
        benchmark.validate_controller_args(args)


def test_validate_controller_args_rejects_invalid_warmups():
    args = SimpleNamespace(
        rows=50_000,
        runs=3,
        warmups=-1,
        minimum_improvement_pct=5.0,
    )

    with pytest.raises(ValueError, match="--warmups"):
        benchmark.validate_controller_args(args)


def test_validate_controller_args_rejects_negative_improvement():
    args = SimpleNamespace(
        rows=50_000,
        runs=3,
        warmups=1,
        minimum_improvement_pct=-1.0,
    )

    with pytest.raises(
        ValueError,
        match="--minimum-improvement-pct",
    ):
        benchmark.validate_controller_args(args)


def test_count_frames():
    class FakeFrame:
        def __init__(self, count):
            self._count = count

        def count(self):
            return self._count

    assert benchmark.count_frames(
        {
            "first": FakeFrame(3),
            "second": FakeFrame(7),
        }
    ) == {"first": 3, "second": 7}


def test_create_parser_defaults():
    args = benchmark.create_parser().parse_args([])

    assert args.rows == 50_000
    assert args.runs == 3
    assert args.warmups == 1
    assert args.master == "local[2]"


def test_run_controller_writes_outputs(
    tmp_path,
    monkeypatch,
):
    def fake_run_worker(command):
        profile = command[
            command.index("--profile") + 1
        ]
        seconds = (
            (10.0, 9.0, 11.0)
            if profile == "baseline"
            else (8.0, 7.0, 9.0)
        )
        return worker_result(profile, seconds=seconds)

    monkeypatch.setattr(
        benchmark,
        "run_worker",
        fake_run_worker,
    )

    args = argparse.Namespace(
        rows=50_000,
        runs=3,
        warmups=1,
        master="local[2]",
        minimum_improvement_pct=5.0,
        output=tmp_path / "result.json",
        history=tmp_path / "history.json",
        markdown=tmp_path / "report.md",
    )

    result = benchmark.run_controller_mode(args)

    assert result["comparison"]["correctness_status"] == (
        "MATCH"
    )
    assert args.output.exists()
    assert args.history.exists()
    assert args.markdown.exists()

