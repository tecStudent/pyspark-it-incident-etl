import argparse
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA_DIR = PROJECT_ROOT / "docs" / "schemas"
DEFAULT_DATA_DIR = PROJECT_ROOT / "docs" / "data"

CONTRACTS = {
    "filter_options": "filter_options.json",
    "daily_trends": "daily_trends.json",
    "risk_summary": "risk_summary.json",
    "forecast_summary": "forecast_summary.json",
    "recommendations": "recommendations.json",
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
    schema_filename = f"{contract_name}.schema.json"

    payload = load_json(data_dir / data_filename)
    schema = load_json(schema_dir / schema_filename)

    validate_payload(
        contract_name=contract_name,
        payload=payload,
        schema=schema,
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
        help="Diretório que contém os cinco JSONs do dashboard.",
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
