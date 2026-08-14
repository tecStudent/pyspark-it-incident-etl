from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_MINIMUM = 50.0
DEFAULT_LOWEST_FILES = 5


class CoverageReportError(ValueError):
    """Raised when a coverage JSON report is missing or malformed."""


def load_coverage_report(path: str | Path) -> dict[str, Any]:
    report_path = Path(path)

    try:
        payload = json.loads(
            report_path.read_text(encoding="utf-8")
        )
    except FileNotFoundError as error:
        raise CoverageReportError(
            f"Relatório de cobertura não encontrado: {report_path}"
        ) from error
    except json.JSONDecodeError as error:
        raise CoverageReportError(
            f"JSON de cobertura inválido: {report_path}"
        ) from error

    if not isinstance(payload, dict):
        raise CoverageReportError(
            "O relatório de cobertura deve ser um objeto JSON."
        )

    return payload


def _mapping(
    value: object,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CoverageReportError(
            f"Campo obrigatório inválido: {field_name}"
        )

    return value


def _number(
    values: Mapping[str, Any],
    field_name: str,
    *,
    default: float | int | None = None,
) -> float:
    value = values.get(field_name, default)

    if isinstance(value, bool) or not isinstance(
        value,
        (int, float),
    ):
        raise CoverageReportError(
            f"Métrica de cobertura inválida: {field_name}"
        )

    return float(value)


def validate_coverage_report(
    report: Mapping[str, Any],
) -> Mapping[str, Any]:
    totals = _mapping(
        report.get("totals"),
        "totals",
    )
    percent = _number(
        totals,
        "percent_covered",
    )

    if not 0 <= percent <= 100:
        raise CoverageReportError(
            "A cobertura total deve estar entre 0 e 100."
        )

    files = report.get("files", {})
    _mapping(files, "files")

    return totals


def total_coverage_percent(
    report: Mapping[str, Any],
) -> float:
    totals = validate_coverage_report(report)
    return _number(totals, "percent_covered")


def meets_minimum(
    report: Mapping[str, Any],
    minimum: float = DEFAULT_MINIMUM,
) -> bool:
    if not 0 <= minimum <= 100:
        raise CoverageReportError(
            "O limite mínimo deve estar entre 0 e 100."
        )

    return total_coverage_percent(report) >= minimum


def lowest_coverage_files(
    report: Mapping[str, Any],
    limit: int = DEFAULT_LOWEST_FILES,
) -> list[tuple[str, float, int]]:
    if limit < 0:
        raise CoverageReportError(
            "A quantidade de arquivos não pode ser negativa."
        )

    files = _mapping(
        report.get("files", {}),
        "files",
    )
    results: list[tuple[str, float, int]] = []

    for file_name, file_payload in files.items():
        payload = _mapping(
            file_payload,
            f"files.{file_name}",
        )
        summary = _mapping(
            payload.get("summary"),
            f"files.{file_name}.summary",
        )
        percent = _number(
            summary,
            "percent_covered",
        )
        missing = int(
            _number(
                summary,
                "missing_lines",
                default=0,
            )
        )
        results.append(
            (str(file_name), percent, missing)
        )

    results.sort(
        key=lambda item: (
            item[1],
            item[0],
        )
    )

    return results[:limit]


def _metric_ratio(
    covered: int,
    total: int,
) -> str:
    if total <= 0:
        return "Não se aplica"

    percent = covered / total * 100
    return f"{covered}/{total} ({percent:.2f}%)"


def _markdown_code(value: str) -> str:
    return f"`{value.replace('`', '')}`"


def build_markdown_summary(
    report: Mapping[str, Any],
    minimum: float = DEFAULT_MINIMUM,
    lowest_limit: int = DEFAULT_LOWEST_FILES,
) -> str:
    totals = validate_coverage_report(report)
    percent = total_coverage_percent(report)
    approved = meets_minimum(report, minimum)

    statements = int(
        _number(totals, "num_statements", default=0)
    )
    covered_lines = int(
        _number(totals, "covered_lines", default=0)
    )
    missing_lines = int(
        _number(totals, "missing_lines", default=0)
    )
    branches = int(
        _number(totals, "num_branches", default=0)
    )
    covered_branches = int(
        _number(totals, "covered_branches", default=0)
    )

    status = "APROVADO" if approved else "REPROVADO"
    icon = "✅" if approved else "❌"

    lines = [
        "## Cobertura de testes",
        "",
        f"{icon} **Quality gate: {status}**",
        "",
        "| Métrica | Resultado |",
        "| --- | ---: |",
        f"| Cobertura total | {percent:.2f}% |",
        (
            "| Linhas executáveis | "
            f"{_metric_ratio(covered_lines, statements)} |"
        ),
        (
            "| Branches | "
            f"{_metric_ratio(covered_branches, branches)} |"
        ),
        f"| Linhas não cobertas | {missing_lines} |",
        f"| Limite mínimo | {minimum:.2f}% |",
    ]

    lowest = lowest_coverage_files(
        report,
        limit=lowest_limit,
    )

    if lowest:
        lines.extend(
            [
                "",
                "### Arquivos com menor cobertura",
                "",
                "| Arquivo | Cobertura | Linhas não cobertas |",
                "| --- | ---: | ---: |",
            ]
        )

        for file_name, file_percent, missing in lowest:
            lines.append(
                "| "
                f"{_markdown_code(file_name)} | "
                f"{file_percent:.2f}% | {missing} |"
            )

    lines.extend(
        [
            "",
            (
                "O relatório HTML completo está disponível "
                "no artefato `coverage-report` desta execução."
            ),
        ]
    )

    return "\n".join(lines) + "\n"


def write_summary(
    summary: str,
    output_path: str | Path,
    *,
    append: bool = False,
) -> None:
    path = Path(output_path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    mode = "a" if append else "w"

    with path.open(
        mode,
        encoding="utf-8",
        newline="\n",
    ) as output_file:
        output_file.write(summary)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Gera um resumo Markdown a partir do coverage.json."
        )
    )
    parser.add_argument(
        "report",
        help="Caminho do arquivo coverage.json.",
    )
    parser.add_argument(
        "--minimum",
        type=float,
        default=DEFAULT_MINIMUM,
        help="Cobertura mínima esperada em porcentagem.",
    )
    parser.add_argument(
        "--lowest-files",
        type=int,
        default=DEFAULT_LOWEST_FILES,
        help="Quantidade de arquivos com menor cobertura no resumo.",
    )
    parser.add_argument(
        "--output",
        help="Arquivo de saída. Sem esta opção, imprime no terminal.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Acrescenta o resumo ao arquivo de saída.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Retorna exit code 1 se a cobertura ficar abaixo do limite.",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)

    try:
        report = load_coverage_report(args.report)
        summary = build_markdown_summary(
            report,
            minimum=args.minimum,
            lowest_limit=args.lowest_files,
        )

        if args.output:
            write_summary(
                summary,
                args.output,
                append=args.append,
            )
        else:
            print(summary, end="")

        if args.check and not meets_minimum(
            report,
            args.minimum,
        ):
            print(
                (
                    "Cobertura abaixo do limite: "
                    f"{total_coverage_percent(report):.2f}% < "
                    f"{args.minimum:.2f}%"
                ),
                file=sys.stderr,
            )
            return 1

    except CoverageReportError as error:
        print(
            f"Falha ao gerar o resumo de cobertura: {error}",
            file=sys.stderr,
        )
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
