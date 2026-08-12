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


def sha256_file(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    digest = hashlib.sha256()

    try:
        with path.open("rb") as source_file:
            for chunk in iter(
                lambda: source_file.read(chunk_size),
                b"",
            ):
                digest.update(chunk)
    except FileNotFoundError as error:
        raise DashboardContractError(
            f"Arquivo não encontrado para o manifesto: {path}"
        ) from error

    return digest.hexdigest()


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
        "size_bytes": data_path.stat().st_size,
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera o manifesto de integridade dos dados "
            "publicados para o dashboard."
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        payload = generate_dashboard_manifest(
            data_dir=args.data_dir,
            schema_dir=args.schema_dir,
            output_path=args.output,
        )
    except DashboardContractError as error:
        print(f"Falha ao gerar o manifesto:\n{error}")
        raise SystemExit(1) from error

    output_path = (
        args.output
        or args.data_dir / "manifest.json"
    )

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
