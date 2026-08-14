import argparse
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError

from src.dashboard_trend_partitions import (
    normalized_file_size,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA_DIR = PROJECT_ROOT / "docs" / "schemas"
DEFAULT_DATA_DIR = PROJECT_ROOT / "docs" / "data"

CONTRACTS = {
    "filter_options": "filter_options.json",
    "daily_trends": "daily_trends_index.json",
    "risk_summary": "risk_summary.json",
    "forecast_summary": "forecast_summary.json",
    "recommendations": "recommendations.json",
}

CONTRACT_SCHEMAS = {
    "filter_options": "filter_options.schema.json",
    "daily_trends": "daily_trends_index.schema.json",
    "risk_summary": "risk_summary.schema.json",
    "forecast_summary": "forecast_summary.schema.json",
    "recommendations": "recommendations.schema.json",
}


class DashboardContractError(ValueError):
    """Indica que um payload não atende ao contrato do dashboard."""


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise DashboardContractError(
            f"Arquivo não encontrado: {path}"
        ) from error
    except json.JSONDecodeError as error:
        raise DashboardContractError(
            f"JSON inválido em {path}: linha {error.lineno}, "
            f"coluna {error.colno}: {error.msg}"
        ) from error


def format_error_path(error: ValidationError) -> str:
    if not error.absolute_path:
        return "$"

    path = "$"
    for part in error.absolute_path:
        if isinstance(part, int):
            path += f"[{part}]"
        else:
            path += f".{part}"

    return path


def validate_payload(
    contract_name: str,
    payload: Any,
    schema: dict[str, Any],
) -> None:
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as error:
        raise DashboardContractError(
            f"Schema inválido para {contract_name}: {error.message}"
        ) from error

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )

    errors = sorted(
        validator.iter_errors(payload),
        key=lambda item: (
            tuple(str(part) for part in item.absolute_path),
            item.message,
        ),
    )

    if not errors:
        return

    details = "\n".join(
        f"- {format_error_path(error)}: {error.message}"
        for error in errors[:10]
    )

    remaining = len(errors) - 10
    if remaining > 0:
        details += f"\n- ... e mais {remaining} erro(s)"

    raise DashboardContractError(
        f"Contrato {contract_name} inválido "
        f"({len(errors)} erro(s)):\n{details}"
    )


def validate_contract(
    contract_name: str,
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
) -> None:
    if contract_name not in CONTRACTS:
        available = ", ".join(sorted(CONTRACTS))
        raise DashboardContractError(
            f"Contrato desconhecido: {contract_name}. "
            f"Disponíveis: {available}"
        )

    data_filename = CONTRACTS[contract_name]
    schema_filename = CONTRACT_SCHEMAS[contract_name]

    payload = load_json(data_dir / data_filename)
    schema = load_json(schema_dir / schema_filename)

    validate_payload(
        contract_name=contract_name,
        payload=payload,
        schema=schema,
    )

    if contract_name == "daily_trends":
        validate_daily_trend_partitions(
            index_payload=payload,
            data_dir=data_dir,
            schema_dir=schema_dir,
        )


def validate_daily_trend_partitions(
    index_payload: dict[str, Any],
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
) -> None:
    partition_schema = load_json(
        schema_dir / "daily_trends.schema.json"
    )
    total_records = 0
    total_size_bytes = 0
    available_partitions = set()

    for entry in index_payload["partitions"]:
        relative_path = Path(entry["path"])
        partition_path = data_dir / relative_path
        partition_payload = load_json(partition_path)

        validate_payload(
            contract_name=(
                "daily_trends "
                f"{entry['year']}-{entry['month']:02d}"
            ),
            payload=partition_payload,
            schema=partition_schema,
        )

        if (
            partition_payload["generated_at"]
            != index_payload["generated_at"]
        ):
            raise DashboardContractError(
                f"generated_at divergente na partição {entry['path']}"
            )

        if partition_payload["mock"] != index_payload["mock"]:
            raise DashboardContractError(
                f"Indicador mock divergente na partição {entry['path']}"
            )

        record_count = len(partition_payload["records"])
        if record_count != entry["record_count"]:
            raise DashboardContractError(
                f"record_count divergente na partição {entry['path']}"
            )

        expected_prefix = (
            f"{entry['year']}-{entry['month']:02d}-"
        )
        if any(
            not record["date"].startswith(expected_prefix)
            for record in partition_payload["records"]
        ):
            raise DashboardContractError(
                f"Registro fora do mês da partição {entry['path']}"
            )

        current_size = normalized_file_size(partition_path)
        if current_size != entry["size_bytes"]:
            raise DashboardContractError(
                f"size_bytes divergente na partição {entry['path']}"
            )

        current_hash = sha256_file(partition_path)
        if current_hash != entry["sha256"]:
            raise DashboardContractError(
                f"sha256 divergente na partição {entry['path']}"
            )

        total_records += record_count
        total_size_bytes += current_size
        available_partitions.add(
            (entry["year"], entry["month"])
        )

    if total_records != index_payload["total_records"]:
        raise DashboardContractError(
            "total_records do índice diverge das partições."
        )

    if total_size_bytes != index_payload["total_size_bytes"]:
        raise DashboardContractError(
            "total_size_bytes do índice diverge das partições."
        )

    if len(index_payload["partitions"]) != index_payload["partition_count"]:
        raise DashboardContractError(
            "partition_count do índice diverge das partições."
        )

    default_partition = index_payload["default_partition"]
    if default_partition is not None:
        default_key = (
            default_partition["year"],
            default_partition["month"],
        )
        if default_key not in available_partitions:
            raise DashboardContractError(
                "default_partition não existe na lista de partições."
            )


def validate_all_contracts(
    data_dir: Path = DEFAULT_DATA_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
) -> list[str]:
    validated = []

    for contract_name in CONTRACTS:
        validate_contract(
            contract_name=contract_name,
            data_dir=data_dir,
            schema_dir=schema_dir,
        )
        validated.append(contract_name)

    return validated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Valida os JSONs do dashboard usando JSON Schema "
            "Draft 2020-12."
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
        help="Diretório que contém os arquivos *.schema.json.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        validated = validate_all_contracts(
            data_dir=args.data_dir,
            schema_dir=args.schema_dir,
        )
    except DashboardContractError as error:
        print(f"Falha na validação dos contratos:\n{error}")
        raise SystemExit(1) from error

    print("Contratos JSON validados com sucesso:")
    for contract_name in validated:
        print(f"- {contract_name}")


if __name__ == "__main__":
    main()
