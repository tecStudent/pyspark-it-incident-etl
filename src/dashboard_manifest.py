import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.validate_dashboard_contracts import (
    CONTRACTS,
    DEFAULT_DATA_DIR,
    DEFAULT_SCHEMA_DIR,
    DashboardContractError,
    load_json,
    validate_contract,
    validate_payload,
)


MANIFEST_SCHEMA_VERSION = "1.0"
DEFAULT_MANIFEST_PATH = DEFAULT_DATA_DIR / "manifest.json"
MANIFEST_SCHEMA_PATH = DEFAULT_SCHEMA_DIR / "manifest.schema.json"

ITEM_COUNT_FIELDS = {
    "filter_options": (
        "years",
        "months",
        "priorities",
        "products",
        "categories",
        "teams",
    ),
    "daily_trends": ("records",),
    "risk_summary": ("items",),
    "forecast_summary": ("forecast",),
    "recommendations": ("items",),
}


def generated_at_utc() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def normalized_file_bytes(path: Path) -> bytes:
    try:
        content = path.read_bytes()
    except FileNotFoundError as error:
        raise DashboardContractError(
            f"Arquivo não encontrado para o manifesto: {path}"
        ) from error

    return (
        content
        .replace(b"\r\n", b"\n")
        .replace(b"\r", b"\n")
    )


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        normalized_file_bytes(path)
    ).hexdigest()


def normalized_file_size(path: Path) -> int:
    return len(normalized_file_bytes(path))


def count_items(
    contract_name: str,
    payload: dict[str, Any],
) -> tuple[int, str]:
    fields = ITEM_COUNT_FIELDS[contract_name]
    total = 0

    for field in fields:
        value = payload.get(field)
        if not isinstance(value, list):
            raise DashboardContractError(
                f"Campo usado na contagem não é uma lista: "
                f"{contract_name}.{field}"
            )
        total += len(value)

    return total, "+".join(fields)


def create_file_entry(
    contract_name: str,
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
) -> dict[str, Any]:
    validate_contract(
        contract_name=contract_name,
        data_dir=data_dir,
        schema_dir=schema_dir,
    )

    filename = CONTRACTS[contract_name]
    data_path = data_dir / filename
    payload = load_json(data_path)

    if not isinstance(payload, dict):
        raise DashboardContractError(
            f"O payload {contract_name} deve ser um objeto JSON."
        )

    item_count, item_count_source = count_items(
        contract_name,
        payload,
    )

    return {
        "name": contract_name,
        "path": filename,
        "schema_path": (
            f"../schemas/{contract_name}.schema.json"
        ),
        "contract_status": "VALID",
        "schema_version": payload["schema_version"],
        "data_generated_at": payload["generated_at"],
        "mock": payload["mock"],
        "item_count": item_count,
        "item_count_source": item_count_source,
        "size_bytes": normalized_file_size(data_path),
        "sha256": sha256_file(data_path),
    }


def create_manifest_payload(
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
    generated_at: str | None = None,
) -> dict[str, Any]:
    files = [
        create_file_entry(
            contract_name=contract_name,
            data_dir=data_dir,
            schema_dir=schema_dir,
        )
        for contract_name in CONTRACTS
    ]

    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": generated_at or generated_at_utc(),
        "status": "HEALTHY",
        "files_total": len(files),
        "files_valid": len(files),
        "contains_mock_data": any(
            file_entry["mock"]
            for file_entry in files
        ),
        "files": files,
    }


def validate_manifest_payload(
    payload: dict[str, Any],
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
) -> None:
    schema = load_json(
        schema_dir / MANIFEST_SCHEMA_PATH.name
    )
    validate_payload(
        contract_name="manifest",
        payload=payload,
        schema=schema,
    )


def write_manifest(
    payload: dict[str, Any],
    output_path: Path = DEFAULT_MANIFEST_PATH,
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    output_path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def generate_dashboard_manifest(
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
    output_path: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    payload = create_manifest_payload(
        data_dir=data_dir,
        schema_dir=schema_dir,
        generated_at=generated_at,
    )
    validate_manifest_payload(
        payload,
        schema_dir=schema_dir,
    )
    write_manifest(
        payload,
        output_path or (data_dir / "manifest.json"),
    )
    return payload


def format_difference_value(value: Any) -> str:
    formatted = repr(value)
    if len(formatted) <= 80:
        return formatted
    return formatted[:77] + "..."


def manifest_differences(
    committed: dict[str, Any],
    expected: dict[str, Any],
) -> list[str]:
    differences = []

    top_level_fields = (
        "schema_version",
        "generated_at",
        "status",
        "files_total",
        "files_valid",
        "contains_mock_data",
    )

    for field in top_level_fields:
        if committed.get(field) != expected.get(field):
            differences.append(
                f"{field}: registrado="
                f"{format_difference_value(committed.get(field))}, "
                f"atual="
                f"{format_difference_value(expected.get(field))}"
            )

    committed_files = committed.get("files", [])
    expected_files = expected.get("files", [])
    committed_names = [
        file_entry.get("name")
        for file_entry in committed_files
    ]
    expected_names = [
        file_entry["name"]
        for file_entry in expected_files
    ]

    if committed_names != expected_names:
        differences.append(
            "files: ordem ou conjunto de payloads diferente; "
            f"registrado={committed_names}, "
            f"atual={expected_names}"
        )

    committed_by_name = {
        file_entry.get("name"): file_entry
        for file_entry in committed_files
    }

    for expected_entry in expected_files:
        contract_name = expected_entry["name"]
        committed_entry = committed_by_name.get(
            contract_name
        )

        if committed_entry is None:
            differences.append(
                f"files.{contract_name}: entrada ausente"
            )
            continue

        for field, expected_value in expected_entry.items():
            committed_value = committed_entry.get(field)
            if committed_value != expected_value:
                differences.append(
                    f"files.{contract_name}.{field}: "
                    f"registrado="
                    f"{format_difference_value(committed_value)}, "
                    f"atual="
                    f"{format_difference_value(expected_value)}"
                )

    return differences


def check_dashboard_manifest(
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    manifest_path = (
        manifest_path
        or data_dir / "manifest.json"
    )
    committed = load_json(manifest_path)

    if not isinstance(committed, dict):
        raise DashboardContractError(
            "O manifesto versionado deve ser um objeto JSON."
        )

    validate_manifest_payload(
        committed,
        schema_dir=schema_dir,
    )

    expected = create_manifest_payload(
        data_dir=data_dir,
        schema_dir=schema_dir,
        generated_at=committed["generated_at"],
    )
    validate_manifest_payload(
        expected,
        schema_dir=schema_dir,
    )

    differences = manifest_differences(
        committed,
        expected,
    )

    if differences:
        details = "\n".join(
            f"- {difference}"
            for difference in differences[:20]
        )
        remaining = len(differences) - 20
        if remaining > 0:
            details += (
                f"\n- ... e mais {remaining} diferença(s)"
            )

        raise DashboardContractError(
            "Manifesto desatualizado em relação aos "
            f"payloads publicados ({len(differences)} "
            f"diferença(s)):\n{details}\n"
            "Regere com: python3 "
            "src/dashboard_manifest.py"
        )

    return committed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera ou verifica o manifesto de integridade "
            "dos dados publicados para o dashboard."
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Diretório que contém os payloads do dashboard.",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=DEFAULT_SCHEMA_DIR,
        help="Diretório que contém os JSON Schemas.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Destino do manifesto. Por padrão, "
            "<data-dir>/manifest.json."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Verifica o manifesto existente sem modificar "
            "nenhum arquivo."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = (
        args.output
        or args.data_dir / "manifest.json"
    )

    try:
        if args.check:
            payload = check_dashboard_manifest(
                data_dir=args.data_dir,
                schema_dir=args.schema_dir,
                manifest_path=output_path,
            )
        else:
            payload = generate_dashboard_manifest(
                data_dir=args.data_dir,
                schema_dir=args.schema_dir,
                output_path=args.output,
            )
    except DashboardContractError as error:
        operation = (
            "verificar"
            if args.check
            else "gerar"
        )
        print(
            f"Falha ao {operation} o manifesto:\n{error}"
        )
        raise SystemExit(1) from error

    if args.check:
        print("Quality gate do manifesto: APROVADO")
        print(f"Manifesto verificado: {output_path}")
        print(f"Status: {payload['status']}")
        print(
            f"Arquivos válidos: "
            f"{payload['files_valid']}/"
            f"{payload['files_total']}"
        )
        return

    print(f"Manifesto gerado: {output_path}")
    print(f"Status: {payload['status']}")
    print(
        f"Arquivos válidos: "
        f"{payload['files_valid']}/"
        f"{payload['files_total']}"
    )
    print(
        "Contém dados simulados: "
        + (
            "SIM"
            if payload["contains_mock_data"]
            else "NÃO"
        )
    )


if __name__ == "__main__":
    main()
