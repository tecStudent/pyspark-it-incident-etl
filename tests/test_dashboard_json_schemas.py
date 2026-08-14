import copy
import json
from pathlib import Path

import pytest

from src.validate_dashboard_contracts import (
    CONTRACTS,
    CONTRACT_SCHEMAS,
    DEFAULT_DATA_DIR,
    DEFAULT_SCHEMA_DIR,
    DashboardContractError,
    load_json,
    validate_all_contracts,
    validate_contract,
    validate_payload,
)


SAMPLE_DATA_DIR = DEFAULT_DATA_DIR / "samples"


@pytest.mark.parametrize("contract_name", CONTRACTS)
def test_production_payloads_follow_json_schema(
    contract_name,
):
    validate_contract(contract_name)


@pytest.mark.parametrize("contract_name", CONTRACTS)
def test_sample_payloads_follow_json_schema(
    contract_name,
):
    validate_contract(
        contract_name,
        data_dir=SAMPLE_DATA_DIR,
    )


def test_validate_all_contracts_returns_validated_names():
    assert validate_all_contracts(
        data_dir=SAMPLE_DATA_DIR,
    ) == list(CONTRACTS)


def test_contract_rejects_missing_required_metadata():
    contract_name = "filter_options"
    payload = load_json(
        SAMPLE_DATA_DIR / CONTRACTS[contract_name]
    )
    schema = load_json(
        DEFAULT_SCHEMA_DIR
        / CONTRACT_SCHEMAS[contract_name]
    )
    invalid_payload = copy.deepcopy(payload)
    invalid_payload.pop("schema_version")

    with pytest.raises(
        DashboardContractError,
        match="schema_version",
    ):
        validate_payload(
            contract_name,
            invalid_payload,
            schema,
        )


def test_contract_rejects_invalid_iso_date():
    contract_name = "daily_trends"
    payload = load_json(
        SAMPLE_DATA_DIR
        / "daily_trends"
        / "2025"
        / "01.json"
    )
    schema = load_json(
        DEFAULT_SCHEMA_DIR
        / f"{contract_name}.schema.json"
    )
    invalid_payload = copy.deepcopy(payload)
    invalid_payload["records"][0]["date"] = "01/01/2025"

    with pytest.raises(
        DashboardContractError,
        match="is not a 'date'",
    ):
        validate_payload(
            contract_name,
            invalid_payload,
            schema,
        )


def test_contract_rejects_unexpected_fields():
    contract_name = "recommendations"
    payload = load_json(
        SAMPLE_DATA_DIR / CONTRACTS[contract_name]
    )
    schema = load_json(
        DEFAULT_SCHEMA_DIR
        / CONTRACT_SCHEMAS[contract_name]
    )
    invalid_payload = copy.deepcopy(payload)
    invalid_payload["unexpected_field"] = True

    with pytest.raises(
        DashboardContractError,
        match="Additional properties are not allowed",
    ):
        validate_payload(
            contract_name,
            invalid_payload,
            schema,
        )


def test_invalid_json_reports_file_and_location(tmp_path):
    invalid_path = tmp_path / "invalid.json"
    invalid_path.write_text(
        '{"schema_version":',
        encoding="utf-8",
    )

    with pytest.raises(
        DashboardContractError,
        match=r"JSON inválido.*linha 1, coluna",
    ):
        load_json(invalid_path)


def test_unknown_contract_has_actionable_message():
    with pytest.raises(
        DashboardContractError,
        match="Contrato desconhecido",
    ):
        validate_contract("unknown_contract")
