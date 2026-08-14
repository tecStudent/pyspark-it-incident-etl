import json
from pathlib import Path

import pytest

from src.dashboard_trend_partitions import (
    TrendPartitionError,
    export_daily_trend_partitions,
    migrate_legacy_daily_trends,
    normalized_file_size,
    sha256_file,
)
from src.validate_dashboard_contracts import (
    DEFAULT_SCHEMA_DIR,
    DashboardContractError,
    validate_contract,
)


GENERATED_AT = "2026-08-14T12:00:00Z"


def trend_record(date_value, total=1):
    return {
        "date": date_value,
        "priority_code": 2,
        "priority_name": "Alta",
        "product": "Hospedagem",
        "category": "Disponibilidade",
        "assigned_group": "Team01",
        "total_incidents": total,
        "kpi_incidents": total,
        "kpi_violations": 0,
        "avg_duration_seconds": 3600.0,
        "p95_duration_seconds": 7200.0,
    }


def test_export_splits_records_by_year_and_month(tmp_path):
    records = [
        trend_record("2024-12-31"),
        trend_record("2025-01-01", 2),
        trend_record("2025-01-02", 3),
        trend_record("2025-02-01", 4),
    ]

    index = export_daily_trend_partitions(
        records=records,
        generated_at=GENERATED_AT,
        data_dir=tmp_path,
    )

    assert index["total_records"] == 4
    assert index["partition_count"] == 3
    assert index["default_partition"] == {
        "year": 2025,
        "month": 2,
    }
    assert [
        entry["path"]
        for entry in index["partitions"]
    ] == [
        "daily_trends/2024/12.json",
        "daily_trends/2025/01.json",
        "daily_trends/2025/02.json",
    ]

    january = json.loads(
        (
            tmp_path
            / "daily_trends"
            / "2025"
            / "01.json"
        ).read_text(encoding="utf-8")
    )
    assert len(january["records"]) == 2


def test_partition_index_contains_current_size_and_hash(tmp_path):
    index = export_daily_trend_partitions(
        records=[trend_record("2025-01-01")],
        generated_at=GENERATED_AT,
        data_dir=tmp_path,
    )

    entry = index["partitions"][0]
    partition_path = tmp_path / entry["path"]

    assert entry["size_bytes"] == normalized_file_size(
        partition_path
    )
    assert entry["sha256"] == sha256_file(partition_path)
    assert index["total_size_bytes"] == entry["size_bytes"]


def test_export_removes_stale_partitions(tmp_path):
    stale_path = (
        tmp_path
        / "daily_trends"
        / "2020"
        / "01.json"
    )
    stale_path.parent.mkdir(parents=True)
    stale_path.write_text("{}", encoding="utf-8")

    export_daily_trend_partitions(
        records=[trend_record("2025-01-01")],
        generated_at=GENERATED_AT,
        data_dir=tmp_path,
    )

    assert not stale_path.exists()
    assert (
        tmp_path
        / "daily_trends"
        / "2025"
        / "01.json"
    ).exists()


def test_migration_removes_monolithic_payload(tmp_path):
    legacy_path = tmp_path / "daily_trends.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "generated_at": GENERATED_AT,
                "mock": False,
                "records": [trend_record("2025-01-01")],
            }
        ),
        encoding="utf-8",
    )

    index = migrate_legacy_daily_trends(
        data_dir=tmp_path,
        regenerate_manifest=False,
    )

    assert index["partition_count"] == 1
    assert not legacy_path.exists()
    assert (tmp_path / "daily_trends_index.json").exists()


def test_invalid_record_date_stops_export(tmp_path):
    with pytest.raises(
        TrendPartitionError,
        match="Data inválida",
    ):
        export_daily_trend_partitions(
            records=[trend_record("14/08/2026")],
            generated_at=GENERATED_AT,
            data_dir=tmp_path,
        )


def test_contract_detects_partition_changed_after_index(tmp_path):
    export_daily_trend_partitions(
        records=[trend_record("2025-01-01")],
        generated_at=GENERATED_AT,
        data_dir=tmp_path,
    )
    partition_path = (
        tmp_path
        / "daily_trends"
        / "2025"
        / "01.json"
    )
    payload = json.loads(
        partition_path.read_text(encoding="utf-8")
    )
    payload["records"][0]["total_incidents"] = 99
    partition_path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    with pytest.raises(
        DashboardContractError,
        match="(size_bytes|sha256) divergente",
    ):
        validate_contract(
            "daily_trends",
            data_dir=tmp_path,
            schema_dir=DEFAULT_SCHEMA_DIR,
        )
