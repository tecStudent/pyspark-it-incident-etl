import json
from pathlib import Path

import pytest

from src.coverage_report import (
    CoverageReportError,
    build_markdown_summary,
    load_coverage_report,
    lowest_coverage_files,
    main,
    meets_minimum,
    total_coverage_percent,
    validate_coverage_report,
    write_summary,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def sample_report(
    percent: float = 75.0,
) -> dict:
    return {
        "meta": {
            "version": "7.0.0",
            "branch_coverage": True,
        },
        "files": {
            "src/high.py": {
                "summary": {
                    "covered_lines": 9,
                    "num_statements": 10,
                    "percent_covered": 90.0,
                    "missing_lines": 1,
                    "excluded_lines": 0,
                    "num_branches": 2,
                    "num_partial_branches": 0,
                    "covered_branches": 2,
                    "missing_branches": 0,
                }
            },
            "src/low.py": {
                "summary": {
                    "covered_lines": 4,
                    "num_statements": 10,
                    "percent_covered": 40.0,
                    "missing_lines": 6,
                    "excluded_lines": 0,
                    "num_branches": 2,
                    "num_partial_branches": 1,
                    "covered_branches": 1,
                    "missing_branches": 1,
                }
            },
        },
        "totals": {
            "covered_lines": 15,
            "num_statements": 20,
            "percent_covered": percent,
            "percent_covered_display": str(round(percent)),
            "missing_lines": 5,
            "excluded_lines": 0,
            "num_branches": 4,
            "num_partial_branches": 1,
            "covered_branches": 3,
            "missing_branches": 1,
        },
    }


def test_load_coverage_report_reads_json(tmp_path):
    report_path = tmp_path / "coverage.json"
    report_path.write_text(
        json.dumps(sample_report()),
        encoding="utf-8",
    )

    loaded = load_coverage_report(report_path)

    assert loaded["totals"]["percent_covered"] == 75.0


def test_load_coverage_report_rejects_missing_file(tmp_path):
    with pytest.raises(
        CoverageReportError,
        match="não encontrado",
    ):
        load_coverage_report(
            tmp_path / "missing.json"
        )


def test_validate_coverage_report_requires_totals():
    with pytest.raises(
        CoverageReportError,
        match="totals",
    ):
        validate_coverage_report(
            {"files": {}}
        )


def test_total_coverage_percent_returns_metric():
    assert total_coverage_percent(
        sample_report(82.35)
    ) == 82.35


def test_meets_minimum_accepts_equal_value():
    assert meets_minimum(
        sample_report(50.0),
        minimum=50.0,
    )


def test_build_markdown_summary_marks_failed_gate():
    summary = build_markdown_summary(
        sample_report(49.99),
        minimum=50.0,
    )

    assert "Quality gate: REPROVADO" in summary
    assert "49.99%" in summary
    assert "50.00%" in summary


def test_build_markdown_summary_contains_metrics():
    summary = build_markdown_summary(
        sample_report(),
    )

    assert "Quality gate: APROVADO" in summary
    assert "15/20 (75.00%)" in summary
    assert "3/4 (75.00%)" in summary


def test_lowest_coverage_files_orders_by_percent():
    lowest = lowest_coverage_files(
        sample_report(),
        limit=2,
    )

    assert lowest == [
        ("src/low.py", 40.0, 6),
        ("src/high.py", 90.0, 1),
    ]


def test_write_summary_supports_append(tmp_path):
    output_path = tmp_path / "reports" / "summary.md"

    write_summary("primeiro\n", output_path)
    write_summary(
        "segundo\n",
        output_path,
        append=True,
    )

    assert output_path.read_text(
        encoding="utf-8"
    ) == "primeiro\nsegundo\n"


def test_main_returns_failure_below_minimum(
    tmp_path,
    capsys,
):
    report_path = tmp_path / "coverage.json"
    report_path.write_text(
        json.dumps(sample_report(40.0)),
        encoding="utf-8",
    )

    exit_code = main(
        [
            str(report_path),
            "--minimum",
            "50",
            "--check",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Cobertura abaixo do limite" in captured.err


def test_ci_runs_coverage_quality_gate():
    workflow = (
        PROJECT_ROOT
        / ".github"
        / "workflows"
        / "ci.yml"
    ).read_text(encoding="utf-8")

    assert "--cov=src" in workflow
    assert "--cov-branch" in workflow
    assert "--cov-fail-under=50" in workflow


def test_ci_publishes_summary_and_artifact():
    workflow = (
        PROJECT_ROOT
        / ".github"
        / "workflows"
        / "ci.yml"
    ).read_text(encoding="utf-8")

    assert "$GITHUB_STEP_SUMMARY" in workflow
    assert "actions/upload-artifact@v7" in workflow
    assert "name: coverage-report" in workflow


def test_ci_prepares_writable_coverage_directory():
    workflow = (
        PROJECT_ROOT
        / ".github"
        / "workflows"
        / "ci.yml"
    ).read_text(encoding="utf-8")

    assert "mkdir -p coverage-artifacts" in workflow
    assert "chmod 0777 coverage-artifacts" in workflow
    assert (
        "COVERAGE_FILE=coverage-artifacts/.coverage"
        in workflow
    )
    assert "coverage-artifacts/coverage.json" in workflow


def test_coverage_config_defines_minimum():
    configuration = (
        PROJECT_ROOT / ".coveragerc"
    ).read_text(encoding="utf-8")

    assert "branch = True" in configuration
    assert "fail_under = 50" in configuration


def test_gitignore_excludes_coverage_outputs():
    gitignore = (
        PROJECT_ROOT / ".gitignore"
    ).read_text(encoding="utf-8")

    assert "coverage.json" in gitignore
    assert "coverage.xml" in gitignore
    assert "htmlcov/" in gitignore
    assert "coverage-artifacts/" in gitignore
