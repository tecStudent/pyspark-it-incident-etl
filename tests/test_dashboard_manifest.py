import hashlib
import json
import shutil

import pytest

from src.dashboard_manifest import (
    ITEM_COUNT_FIELDS,
    create_file_entry,
    create_manifest_payload,
    generate_dashboard_manifest,
    sha256_file,
    validate_manifest_payload,
)
from src.validate_dashboard_contracts import (
    CONTRACTS,
    DEFAULT_DATA_DIR,
    DEFAULT_SCHEMA_DIR,
    DashboardContractError,
)


SAMPLE_DATA_DIR = DEFAULT_DATA_DIR / "samples"
GENERATED_AT = "2026-08-12T20:00:00Z"

EXPECTED_ITEM_COUNTS = {
    "filter_options": 13,
    "daily_trends": 3,
    "risk_summary": 4,
    "forecast_summary": 7,
    "recommendations": 4,
}


@pytest.mark.parametrize(
    ("contract_name", "expected_count"),
    EXPECTED_ITEM_COUNTS.items(),
)
def test_file_entries_include_counts_and_integrity(
    contract_name,
    expected_count,
):
    entry = create_file_entry(
        contract_name=contract_name,
        data_dir=SAMPLE_DATA_DIR,
    )

    assert entry["contract_status"] == "VALID"
    assert entry["item_count"] == expected_count
    expected_source = (
        "total_records"
        if contract_name == "daily_trends"
        else "+".join(ITEM_COUNT_FIELDS[contract_name])
    )
    assert entry["item_count_source"] == expected_source
    assert entry["size_bytes"] > 0
    assert len(entry["sha256"]) == 64
    assert entry["mock"] is True


def test_manifest_describes_all_contracts():
    payload = create_manifest_payload(
        data_dir=SAMPLE_DATA_DIR,
        generated_at=GENERATED_AT,
    )

    assert payload["schema_version"] == "1.0"
    assert payload["generated_at"] == GENERATED_AT
    assert payload["status"] == "HEALTHY"
    assert payload["files_total"] == len(CONTRACTS)
    assert payload["files_valid"] == len(CONTRACTS)
    assert payload["contains_mock_data"] is True
    assert [
        item["name"]
        for item in payload["files"]
    ] == list(CONTRACTS)


def test_manifest_payload_follows_its_json_schema():
    payload = create_manifest_payload(
        data_dir=SAMPLE_DATA_DIR,
        generated_at=GENERATED_AT,
    )

    validate_manifest_payload(payload)


def test_generate_manifest_writes_valid_json(tmp_path):
    output_path = tmp_path / "manifest.json"

    payload = generate_dashboard_manifest(
        data_dir=SAMPLE_DATA_DIR,
        output_path=output_path,
        generated_at=GENERATED_AT,
    )

    assert output_path.exists()
    assert (
        json.loads(output_path.read_text(encoding="utf-8"))
        == payload
    )
    assert output_path.read_bytes().endswith(b"\n")


def test_sha256_matches_file_content(tmp_path):
    source_path = tmp_path / "payload.json"
    content = b'{"status":"VALID"}\n'
    source_path.write_bytes(content)

    assert sha256_file(source_path) == (
        hashlib.sha256(content).hexdigest()
    )


def test_missing_payload_prevents_manifest_generation(
    tmp_path,
):
    data_dir = tmp_path / "data"
    shutil.copytree(SAMPLE_DATA_DIR, data_dir)
    (data_dir / "risk_summary.json").unlink()

    with pytest.raises(
        DashboardContractError,
        match="Arquivo não encontrado",
    ):
        create_manifest_payload(data_dir=data_dir)


def test_invalid_payload_prevents_healthy_manifest(
    tmp_path,
):
    data_dir = tmp_path / "data"
    shutil.copytree(SAMPLE_DATA_DIR, data_dir)
    daily_path = (
        data_dir
        / "daily_trends"
        / "2025"
        / "01.json"
    )
    daily_payload = json.loads(
        daily_path.read_text(encoding="utf-8")
    )
    daily_payload["records"][0]["date"] = "01/01/2025"
    daily_path.write_text(
        json.dumps(daily_payload),
        encoding="utf-8",
    )

    with pytest.raises(
        DashboardContractError,
        match="daily_trends",
    ):
        create_manifest_payload(data_dir=data_dir)


def test_manifest_schema_rejects_invalid_hash():
    payload = create_manifest_payload(
        data_dir=SAMPLE_DATA_DIR,
        generated_at=GENERATED_AT,
    )
    payload["files"][0]["sha256"] = "invalid"

    with pytest.raises(
        DashboardContractError,
        match="sha256",
    ):
        validate_manifest_payload(payload)
