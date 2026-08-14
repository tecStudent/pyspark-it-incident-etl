import argparse
import hashlib
import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1.0"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "docs" / "data"
DEFAULT_LEGACY_PATH = DEFAULT_DATA_DIR / "daily_trends.json"
DEFAULT_INDEX_PATH = DEFAULT_DATA_DIR / "daily_trends_index.json"
DEFAULT_PARTITION_DIR = DEFAULT_DATA_DIR / "daily_trends"


class TrendPartitionError(ValueError):
    """Indica que as tendências não podem ser particionadas com segurança."""


def normalized_file_bytes(path: Path) -> bytes:
    return (
        path.read_bytes()
        .replace(b"\r\n", b"\n")
        .replace(b"\r", b"\n")
    )


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        normalized_file_bytes(path)
    ).hexdigest()


def normalized_file_size(path: Path) -> int:
    return len(normalized_file_bytes(path))


def write_json_atomic(
    output_path: Path,
    payload: dict[str, Any],
    *,
    compact: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(
        output_path.suffix + ".tmp"
    )

    options: dict[str, Any] = {
        "ensure_ascii": False,
        "allow_nan": False,
    }
    if compact:
        options["separators"] = (",", ":")
    else:
        options["indent"] = 2

    temporary_path.write_text(
        json.dumps(payload, **options) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(output_path)


def record_partition(record: dict[str, Any]) -> tuple[int, int]:
    raw_date = record.get("date")
    if not isinstance(raw_date, str):
        raise TrendPartitionError(
            "Registro de tendência sem data no formato texto."
        )

    try:
        parsed_date = date.fromisoformat(raw_date)
    except ValueError as error:
        raise TrendPartitionError(
            f"Data inválida no registro de tendência: {raw_date!r}."
        ) from error

    return parsed_date.year, parsed_date.month


def group_records_by_month(
    records: list[dict[str, Any]],
) -> dict[tuple[int, int], list[dict[str, Any]]]:
    partitions: dict[
        tuple[int, int],
        list[dict[str, Any]],
    ] = {}

    for record in records:
        if not isinstance(record, dict):
            raise TrendPartitionError(
                "Todos os registros de tendência devem ser objetos JSON."
            )
        key = record_partition(record)
        partitions.setdefault(key, []).append(record)

    for partition_records in partitions.values():
        partition_records.sort(
            key=lambda item: (
                item["date"],
                item.get("priority_code") or 0,
                item.get("product") or "",
                item.get("category") or "",
                item.get("assigned_group") or "",
            )
        )

    return partitions


def clean_partition_directory(partition_dir: Path) -> None:
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    partition_dir.mkdir(parents=True, exist_ok=True)


def export_daily_trend_partitions(
    records: list[dict[str, Any]],
    generated_at: str,
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    mock: bool = False,
    remove_legacy: bool = True,
) -> dict[str, Any]:
    partition_dir = data_dir / "daily_trends"
    index_path = data_dir / "daily_trends_index.json"
    legacy_path = data_dir / "daily_trends.json"
    grouped_records = group_records_by_month(records)

    clean_partition_directory(partition_dir)
    partition_entries = []

    for (year, month), partition_records in sorted(
        grouped_records.items()
    ):
        relative_path = (
            Path("daily_trends")
            / str(year)
            / f"{month:02d}.json"
        )
        output_path = data_dir / relative_path
        payload = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
            "mock": mock,
            "records": partition_records,
        }
        write_json_atomic(
            output_path,
            payload,
            compact=True,
        )
        partition_entries.append(
            {
                "year": year,
                "month": month,
                "path": relative_path.as_posix(),
                "record_count": len(partition_records),
                "size_bytes": normalized_file_size(output_path),
                "sha256": sha256_file(output_path),
            }
        )

    latest_partition = (
        partition_entries[-1]
        if partition_entries
        else None
    )
    index_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "mock": mock,
        "total_records": len(records),
        "total_size_bytes": sum(
            entry["size_bytes"]
            for entry in partition_entries
        ),
        "partition_count": len(partition_entries),
        "default_partition": (
            {
                "year": latest_partition["year"],
                "month": latest_partition["month"],
            }
            if latest_partition
            else None
        ),
        "partitions": partition_entries,
    }
    write_json_atomic(index_path, index_payload)

    if remove_legacy and legacy_path.exists():
        legacy_path.unlink()

    return index_payload


def load_legacy_payload(source_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(
            source_path.read_text(encoding="utf-8")
        )
    except FileNotFoundError as error:
        raise TrendPartitionError(
            f"Arquivo legado não encontrado: {source_path}"
        ) from error
    except json.JSONDecodeError as error:
        raise TrendPartitionError(
            f"JSON legado inválido: {error.msg}"
        ) from error

    if not isinstance(payload, dict):
        raise TrendPartitionError(
            "O payload legado deve ser um objeto JSON."
        )

    records = payload.get("records")
    if not isinstance(records, list):
        raise TrendPartitionError(
            "O payload legado não contém uma lista records."
        )

    if not isinstance(payload.get("generated_at"), str):
        raise TrendPartitionError(
            "O payload legado não contém generated_at válido."
        )

    return payload


def migrate_legacy_daily_trends(
    data_dir: Path = DEFAULT_DATA_DIR,
    *,
    regenerate_manifest: bool = True,
) -> dict[str, Any]:
    source_path = data_dir / "daily_trends.json"
    payload = load_legacy_payload(source_path)
    index_payload = export_daily_trend_partitions(
        records=payload["records"],
        generated_at=payload["generated_at"],
        data_dir=data_dir,
        mock=bool(payload.get("mock", False)),
        remove_legacy=True,
    )

    if regenerate_manifest:
        from src.dashboard_manifest import generate_dashboard_manifest

        generate_dashboard_manifest(data_dir=data_dir)

    return index_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Particiona daily_trends.json por ano e mês e "
            "atualiza o manifesto do dashboard."
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Diretório docs/data ou equivalente.",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="Não regenera o manifesto após a migração.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        index_payload = migrate_legacy_daily_trends(
            data_dir=args.data_dir,
            regenerate_manifest=not args.skip_manifest,
        )
    except TrendPartitionError as error:
        print(f"Falha ao particionar tendências: {error}")
        raise SystemExit(1) from error

    print("Tendências particionadas com sucesso")
    print(
        f"Partições: {index_payload['partition_count']}"
    )
    print(
        f"Registros: {index_payload['total_records']}"
    )
    print(
        "Tamanho publicado: "
        f"{index_payload['total_size_bytes']} bytes"
    )
    print(
        "Índice: "
        f"{args.data_dir / 'daily_trends_index.json'}"
    )


if __name__ == "__main__":
    main()
