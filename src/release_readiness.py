import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SEMVER_PATTERN = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$"
)
REQUIRED_SCREENSHOTS = (
    "docs/assets/dashboard-preview.png",
    "docs/assets/dashboard-recommendations.png",
)


def read_version(root: Path = ROOT) -> str:
    return (root / "VERSION").read_text(encoding="utf-8").strip()


def is_stable_semver(version: str) -> bool:
    return SEMVER_PATTERN.fullmatch(version) is not None


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as json_file:
        return json.load(json_file)


def make_check(
    name: str,
    passed: bool,
    details: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "PASS" if passed else "FAIL",
        "details": details,
    }


def validate_release(
    root: Path = ROOT,
    expected_version: str | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    version_path = root / "VERSION"
    version = read_version(root) if version_path.is_file() else ""
    version_valid = is_stable_semver(version)
    checks.append(
        make_check(
            "semantic_version",
            version_valid,
            version or "VERSION ausente ou vazio",
        )
    )

    expected_matches = expected_version is None or version == expected_version
    checks.append(
        make_check(
            "expected_version",
            expected_matches,
            f"declarada={version!r}, esperada={expected_version!r}",
        )
    )

    changelog_path = root / "CHANGELOG.md"
    changelog = (
        changelog_path.read_text(encoding="utf-8")
        if changelog_path.is_file()
        else ""
    )
    changelog_valid = (
        "## [Unreleased]" in changelog
        and f"## [{version}] - " in changelog
    )
    checks.append(
        make_check(
            "changelog",
            changelog_valid,
            f"entrada da versão {version or 'indisponível'}",
        )
    )

    release_notes_path = root / "docs" / "releases" / f"v{version}.md"
    release_notes = (
        release_notes_path.read_text(encoding="utf-8")
        if release_notes_path.is_file()
        else ""
    )
    required_sections = (
        "## Destaques",
        "## Qualidade da release",
        "## Executar localmente",
        "## Limitações conhecidas",
    )
    release_notes_valid = (
        release_notes.startswith(f"# v{version}")
        and all(section in release_notes for section in required_sections)
    )
    checks.append(
        make_check(
            "release_notes",
            release_notes_valid,
            str(release_notes_path.relative_to(root)),
        )
    )

    readme_path = root / "README.md"
    readme = (
        readme_path.read_text(encoding="utf-8")
        if readme_path.is_file()
        else ""
    )
    readme_valid = all(
        expected in readme
        for expected in (
            f"releases/tag/v{version}",
            "docs/assets/dashboard-preview.png",
            "docs/assets/dashboard-recommendations.png",
            "[CHANGELOG.md](CHANGELOG.md)",
            "[RELEASING.md](RELEASING.md)",
        )
    )
    checks.append(
        make_check(
            "readme_release_links",
            readme_valid,
            "badge, screenshots, changelog e processo de release",
        )
    )

    screenshot_details = []
    screenshots_valid = True
    for relative_path in REQUIRED_SCREENSHOTS:
        screenshot_path = root / relative_path
        size = screenshot_path.stat().st_size if screenshot_path.is_file() else 0
        screenshots_valid = screenshots_valid and size >= 50_000
        screenshot_details.append(f"{relative_path}={size} bytes")
    checks.append(
        make_check(
            "release_screenshots",
            screenshots_valid,
            ", ".join(screenshot_details),
        )
    )

    manifest_path = root / "docs" / "data" / "manifest.json"
    try:
        manifest = read_json(manifest_path)
    except (FileNotFoundError, json.JSONDecodeError):
        manifest = {}
    manifest_valid = (
        manifest.get("status") == "HEALTHY"
        and manifest.get("files_total") == manifest.get("files_valid")
        and manifest.get("files_total", 0) > 0
        and manifest.get("contains_mock_data") is False
    )
    checks.append(
        make_check(
            "dashboard_manifest",
            manifest_valid,
            (
                f"status={manifest.get('status')!r}, "
                f"válidos={manifest.get('files_valid')}/"
                f"{manifest.get('files_total')}, "
                f"mock={manifest.get('contains_mock_data')!r}"
            ),
        )
    )

    required_files = (
        "LICENSE",
        "CONTRIBUTING.md",
        "CODE_OF_CONDUCT.md",
        "RELEASING.md",
        ".github/release.yml",
    )
    missing_files = [
        relative_path
        for relative_path in required_files
        if not (root / relative_path).is_file()
    ]
    checks.append(
        make_check(
            "release_governance",
            not missing_files,
            (
                "todos os arquivos presentes"
                if not missing_files
                else f"ausentes: {', '.join(missing_files)}"
            ),
        )
    )

    failed = [check for check in checks if check["status"] == "FAIL"]
    return {
        "schema_version": "1.0",
        "release_version": version,
        "tag": f"v{version}" if version else None,
        "status": "READY" if not failed else "BLOCKED",
        "checks_total": len(checks),
        "checks_passed": len(checks) - len(failed),
        "checks_failed": len(failed),
        "checks": checks,
    }


def print_report(report: dict[str, Any]) -> None:
    approved = report["status"] == "READY"
    print(
        "Release readiness: "
        + ("APROVADO" if approved else "REPROVADO")
    )
    print(f"Versão: {report['release_version'] or 'indisponível'}")
    print(
        "Checks: "
        f"{report['checks_passed']}/{report['checks_total']} aprovados"
    )
    for check in report["checks"]:
        marker = "OK" if check["status"] == "PASS" else "FALHA"
        print(f"- [{marker}] {check['name']}: {check['details']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verifica a preparação de uma release estável."
    )
    parser.add_argument(
        "--version",
        help="Versão esperada sem o prefixo v.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Retorna exit code diferente de zero quando houver falhas.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Caminho opcional para gravar o relatório JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = validate_release(
        root=ROOT,
        expected_version=args.version,
    )
    print_report(report)

    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    if args.check and report["status"] != "READY":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

