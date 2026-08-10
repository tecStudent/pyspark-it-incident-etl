import csv
import re
from collections import Counter
from pathlib import Path
from typing import TextIO


INPUT_PATH = Path("data/raw/incidents.csv")
OUTPUT_DIR = Path("data/raw/batches")

OPENED_COLUMN = "Aberto"
DATE_PATTERN = re.compile(r"^(\d{4})-(\d{2})")


def create_batches() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Arquivo de origem não encontrado: {INPUT_PATH}"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_files: dict[str, TextIO] = {}
    writers: dict[str, csv.DictWriter] = {}

    batch_counts = Counter()
    total_records = 0

    try:
        with INPUT_PATH.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as input_file:

            reader = csv.DictReader(input_file)

            if not reader.fieldnames:
                raise ValueError(
                    "O arquivo de origem não possui cabeçalho."
                )

            if OPENED_COLUMN not in reader.fieldnames:
                raise ValueError(
                    f"Coluna obrigatória ausente: {OPENED_COLUMN}"
                )

            for line_number, row in enumerate(
                reader,
                start=2,
            ):
                opened_at = (
                    row.get(OPENED_COLUMN)
                    or ""
                ).strip()

                match = DATE_PATTERN.match(opened_at)

                if not match:
                    raise ValueError(
                        "Data de abertura inválida "
                        f"na linha {line_number}: {opened_at}"
                    )

                year, month = match.groups()

                batch_id = f"{year}_{month}"

                if batch_id not in writers:
                    output_path = (
                        OUTPUT_DIR
                        / f"incidents_{batch_id}.csv"
                    )

                    output_file = output_path.open(
                        "w",
                        encoding="utf-8",
                        newline="",
                    )

                    writer = csv.DictWriter(
                        output_file,
                        fieldnames=reader.fieldnames,
                    )

                    writer.writeheader()

                    output_files[batch_id] = output_file
                    writers[batch_id] = writer

                writers[batch_id].writerow(row)

                batch_counts[batch_id] += 1
                total_records += 1

    finally:
        for output_file in output_files.values():
            output_file.close()

    print(
        f"Registros distribuídos: {total_records}"
    )

    print(
        f"Lotes mensais gerados: {len(batch_counts)}"
    )

    for batch_id in sorted(batch_counts):
        print(
            f"incidents_{batch_id}.csv: "
            f"{batch_counts[batch_id]} registros"
        )


def main() -> None:
    create_batches()


if __name__ == "__main__":
    main()