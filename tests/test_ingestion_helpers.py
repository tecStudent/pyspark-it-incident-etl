import csv
from pathlib import Path

import pytest
from openpyxl import Workbook
from pyspark.sql.types import StringType

from src import bronze
from src import create_incremental_batches
from src import extract_xlsx


def create_workbook(
    path: Path,
    sheet_name: str = "Dataset Geral",
) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = sheet_name
    worksheet.append(
        [
            "Número",
            "Aberto",
        ]
    )
    worksheet.append(
        [
            "INC0001",
            "2025-01-10 08:00:00",
        ]
    )
    worksheet.append(
        [
            "INC0002",
            "2025-02-10 09:00:00",
        ]
    )
    workbook.save(path)
    workbook.close()


def write_incident_csv(
    path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str] | None = None,
) -> None:
    columns = fieldnames or [
        "Número",
        "Aberto",
    ]

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=columns,
        )
        writer.writeheader()
        writer.writerows(rows)


def test_extract_excel_writes_csv(
    tmp_path,
    capsys,
):
    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output" / "incidents.csv"
    create_workbook(input_path)

    extract_xlsx.extract_excel(
        input_path=input_path,
        output_path=output_path,
        sheet_name="Dataset Geral",
    )

    with output_path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as csv_file:
        rows = list(csv.reader(csv_file))

    captured = capsys.readouterr()
    assert rows == [
        ["Número", "Aberto"],
        ["INC0001", "2025-01-10 08:00:00"],
        ["INC0002", "2025-02-10 09:00:00"],
    ]
    assert "Registros extraídos: 2" in captured.out


def test_extract_excel_rejects_missing_input(
    tmp_path,
):
    with pytest.raises(
        FileNotFoundError,
        match="Arquivo não encontrado",
    ):
        extract_xlsx.extract_excel(
            input_path=tmp_path / "missing.xlsx",
            output_path=tmp_path / "output.csv",
            sheet_name="Dataset Geral",
        )


def test_extract_excel_rejects_missing_sheet(
    tmp_path,
):
    input_path = tmp_path / "input.xlsx"
    create_workbook(
        input_path,
        sheet_name="Outra Aba",
    )

    with pytest.raises(
        ValueError,
        match="não encontrada",
    ):
        extract_xlsx.extract_excel(
            input_path=input_path,
            output_path=tmp_path / "output.csv",
            sheet_name="Dataset Geral",
        )


def test_extract_main_passes_command_line_arguments(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.csv"
    received = {}

    def fake_extract_excel(**kwargs):
        received.update(kwargs)

    monkeypatch.setattr(
        extract_xlsx,
        "extract_excel",
        fake_extract_excel,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "extract_xlsx.py",
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--sheet",
            "Aba Teste",
        ],
    )

    extract_xlsx.main()

    assert received == {
        "input_path": input_path,
        "output_path": output_path,
        "sheet_name": "Aba Teste",
    }


def test_create_batches_writes_monthly_files(
    tmp_path,
    monkeypatch,
    capsys,
):
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "batches"
    write_incident_csv(
        input_path,
        [
            {
                "Número": "INC0001",
                "Aberto": "2025-01-10 08:00:00",
            },
            {
                "Número": "INC0002",
                "Aberto": "2025-01-11 09:00:00",
            },
            {
                "Número": "INC0003",
                "Aberto": "2025-02-01 10:00:00",
            },
        ],
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "INPUT_PATH",
        input_path,
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "OUTPUT_DIR",
        output_dir,
    )

    create_incremental_batches.create_batches()

    january_path = output_dir / "incidents_2025_01.csv"
    february_path = output_dir / "incidents_2025_02.csv"
    captured = capsys.readouterr()

    assert january_path.exists()
    assert february_path.exists()
    assert len(
        january_path.read_text(
            encoding="utf-8",
        ).splitlines()
    ) == 3
    assert len(
        february_path.read_text(
            encoding="utf-8",
        ).splitlines()
    ) == 2
    assert "Registros distribuídos: 3" in captured.out
    assert "Lotes mensais gerados: 2" in captured.out


def test_create_batches_rejects_missing_input(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        create_incremental_batches,
        "INPUT_PATH",
        tmp_path / "missing.csv",
    )

    with pytest.raises(
        FileNotFoundError,
        match="Arquivo de origem não encontrado",
    ):
        create_incremental_batches.create_batches()


def test_create_batches_rejects_empty_file(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "empty.csv"
    input_path.write_text(
        "",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "INPUT_PATH",
        input_path,
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "OUTPUT_DIR",
        tmp_path / "batches",
    )

    with pytest.raises(
        ValueError,
        match="não possui cabeçalho",
    ):
        create_incremental_batches.create_batches()


def test_create_batches_requires_opened_column(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "incidents.csv"
    write_incident_csv(
        input_path,
        [{"Número": "INC0001"}],
        fieldnames=["Número"],
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "INPUT_PATH",
        input_path,
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "OUTPUT_DIR",
        tmp_path / "batches",
    )

    with pytest.raises(
        ValueError,
        match="Coluna obrigatória ausente",
    ):
        create_incremental_batches.create_batches()


def test_create_batches_rejects_invalid_date(
    tmp_path,
    monkeypatch,
):
    input_path = tmp_path / "incidents.csv"
    output_dir = tmp_path / "batches"
    write_incident_csv(
        input_path,
        [
            {
                "Número": "INC0001",
                "Aberto": "2025-01-10 08:00:00",
            },
            {
                "Número": "INC0002",
                "Aberto": "data inválida",
            },
        ],
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "INPUT_PATH",
        input_path,
    )
    monkeypatch.setattr(
        create_incremental_batches,
        "OUTPUT_DIR",
        output_dir,
    )

    with pytest.raises(
        ValueError,
        match="Data de abertura inválida",
    ):
        create_incremental_batches.create_batches()

    january_path = output_dir / "incidents_2025_01.csv"
    assert january_path.read_text(encoding="utf-8")


def test_create_batches_main_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        create_incremental_batches,
        "create_batches",
        lambda: calls.append("called"),
    )

    create_incremental_batches.main()

    assert calls == ["called"]


def test_bronze_schema_preserves_source_columns():
    field_names = [
        field.name
        for field in bronze.RAW_SCHEMA.fields
    ]

    assert len(field_names) == 19
    assert field_names[0] == "Número"
    assert field_names[-1] == "KPI Violado?"
    assert all(
        isinstance(field.dataType, StringType)
        for field in bronze.RAW_SCHEMA.fields
    )


def test_bronze_create_spark_session_reuses_context(
    spark,
):
    created_session = bronze.create_spark_session()

    assert (
        created_session.sparkContext
        is spark.sparkContext
    )
