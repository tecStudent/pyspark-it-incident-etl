import json
import shutil

import pytest

from src.dashboard_manifest import (
    check_dashboard_manifest,
    generate_dashboard_manifest,
    manifest_differences,
    normalized_file_size,
    sha256_file,
)
from src.validate_dashboard_contracts import (
    DEFAULT_DATA_DIR,
    DashboardContractError,
)


SAMPLE_DATA_DIR = DEFAULT_DATA_DIR / "samples"
GENERATED_AT = "2026-08-13T12:00:00Z"


def prepare_data_and_manifest(tmp_path):
    data_dir = tmp_path / "data"
    shutil.copytree(SAMPLE_DATA_DIR, data_dir)
    manifest_path = data_dir / "manifest.json"
    generate_dashboard_manifest(
        data_dir=data_dir,
        output_path=manifest_path,
        generated_at=GENERATED_AT,
    )
    return data_dir, manifest_path


def overwrite_manifest(manifest_path, payload):
    manifest_path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def test_file_metrics_are_stable_across_line_endings(
    tmp_path,
):
    payload_path = tmp_path / "payload.json"
    lf_content = b'{\n  "status": "VALID"\n}\n'
    crlf_content = lf_content.replace(b"\n", b"\r\n")

    payload_path.write_bytes(lf_content)
    lf_hash = sha256_file(payload_path)
    lf_size = normalized_file_size(payload_path)

    payload_path.write_bytes(crlf_content)

    assert sha256_file(payload_path) == lf_hash
    assert normalized_file_size(payload_path) == lf_size


def test_check_accepts_current_manifest(tmp_path):
    data_dir, manifest_path = prepare_data_and_manifest(
        tmp_path
    )

    payload = check_dashboard_manifest(
        data_dir=data_dir,
        manifest_path=manifest_path,
    )

    assert payload["status"] == "HEALTHY"
    assert payload["files_valid"] == 5


def test_check_does_not_rewrite_manifest(tmp_path):
    data_dir, manifest_path = prepare_data_and_manifest(
        tmp_path
    )
    content_before = manifest_path.read_bytes()

    check_dashboard_manifest(
        data_dir=data_dir,
        manifest_path=manifest_path,
    )

    assert manifest_path.read_bytes() == content_before


def test_check_detects_payload_changed_after_generation(
    tmp_path,
):
    data_dir, manifest_path = prepare_data_and_manifest(
        tmp_path
    )
    recommendations_path = (
        data_dir / "recommendations.json"
    )
    recommendations = json.loads(
        recommendations_path.read_text(encoding="utf-8")
    )
    recommendations["items"][0]["title"] = (
        "Título alterado após a geração do manifesto"
    )
    recommendations_path.write_text(
        json.dumps(
            recommendations,
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        DashboardContractError,
        match=r"files\.recommendations\.(size_bytes|sha256)",
    ):
        check_dashboard_manifest(
            data_dir=data_dir,
            manifest_path=manifest_path,
        )


@pytest.mark.parametrize(
    ("field", "stale_value"),
    [
        ("sha256", "0" * 64),
        ("size_bytes", 1),
        ("item_count", 999),
    ],
)
def test_check_detects_stale_file_metadata(
    tmp_path,
    field,
    stale_value,
):
    data_dir, manifest_path = prepare_data_and_manifest(
        tmp_path
    )
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest["files"][0][field] = stale_value
    overwrite_manifest(manifest_path, manifest)

    with pytest.raises(
        DashboardContractError,
        match=rf"files\.filter_options\.{field}",
    ):
        check_dashboard_manifest(
            data_dir=data_dir,
            manifest_path=manifest_path,
        )


def test_check_rejects_missing_manifest(tmp_path):
    data_dir = tmp_path / "data"
    shutil.copytree(SAMPLE_DATA_DIR, data_dir)

    with pytest.raises(
        DashboardContractError,
        match="Arquivo não encontrado",
    ):
        check_dashboard_manifest(data_dir=data_dir)


def test_check_rejects_invalid_manifest_schema(
    tmp_path,
):
    data_dir, manifest_path = prepare_data_and_manifest(
        tmp_path
    )
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest["status"] = "BROKEN"
    overwrite_manifest(manifest_path, manifest)

    with pytest.raises(
        DashboardContractError,
        match="manifest",
    ):
        check_dashboard_manifest(
            data_dir=data_dir,
            manifest_path=manifest_path,
        )


def test_check_detects_missing_file_entry(tmp_path):
    data_dir, manifest_path = prepare_data_and_manifest(
        tmp_path
    )
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest["files"] = [
        file_entry
        for file_entry in manifest["files"]
        if file_entry["name"] != "risk_summary"
    ]
    overwrite_manifest(manifest_path, manifest)

    with pytest.raises(
        DashboardContractError,
        match=r"files\.risk_summary: entrada ausente",
    ):
        check_dashboard_manifest(
            data_dir=data_dir,
            manifest_path=manifest_path,
        )


def test_manifest_difference_reports_top_level_changes():
    committed = {
        "schema_version": "1.0",
        "generated_at": GENERATED_AT,
        "status": "HEALTHY",
        "files_total": 5,
        "files_valid": 5,
        "contains_mock_data": False,
        "files": [],
    }
    expected = {
        **committed,
        "contains_mock_data": True,
    }

    differences = manifest_differences(
        committed,
        expected,
    )

    assert any(
        difference.startswith("contains_mock_data:")
        for difference in differences
    )
