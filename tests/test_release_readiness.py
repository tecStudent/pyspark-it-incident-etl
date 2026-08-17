import subprocess
import sys
from pathlib import Path

from src.release_readiness import (
    is_stable_semver,
    read_version,
    validate_release,
)


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_version_file_declares_current_stable_release():
    assert read_version(ROOT) == "1.1.0"


def test_release_version_uses_stable_semver():
    assert is_stable_semver("1.0.0")
    assert is_stable_semver("2.15.3")


def test_invalid_release_versions_are_rejected():
    invalid_versions = ("v1.0.0", "1.0", "01.0.0", "1.0.0-beta")

    assert not any(is_stable_semver(version) for version in invalid_versions)


def test_changelog_has_unreleased_and_release_history():
    changelog = read("CHANGELOG.md")

    assert "## [Unreleased]" in changelog
    assert "## [1.1.0] - 2026-08-17" in changelog
    assert "- 278 testes automatizados aprovados em ambiente Docker." in changelog
    assert "## [1.0.0] - 2026-08-14" in changelog
    assert "219 testes automatizados" in changelog


def test_release_notes_have_required_sections():
    notes = read("docs/releases/v1.1.0.md")

    assert notes.startswith("# v1.1.0")
    assert "## Destaques" in notes
    assert "## Qualidade da release" in notes
    assert "## Limitações conhecidas" in notes


def test_release_documentation_uses_annotated_tag():
    releasing = read("RELEASING.md")

    assert 'git tag -a v1.1.0 -m "Release v1.1.0"' in releasing
    assert "git push origin v1.1.0" in releasing
    assert "Set as the latest release" in releasing
    assert "Set as a pre-release" in releasing


def test_release_configuration_groups_generated_notes():
    configuration = read(".github/release.yml")

    assert "changelog:" in configuration
    assert "Novas funcionalidades" in configuration
    assert "Correções" in configuration
    assert 'labels:\n        - "*"' in configuration


def test_readme_exposes_release_badge_and_links():
    readme = read("README.md")

    assert "releases/tag/v1.1.0" in readme
    assert "[CHANGELOG.md](CHANGELOG.md)" in readme
    assert "[RELEASING.md](RELEASING.md)" in readme


def test_release_screenshots_are_present_and_nonempty():
    screenshots = (
        ROOT / "docs/assets/dashboard-preview.png",
        ROOT / "docs/assets/dashboard-recommendations.png",
    )

    assert all(path.is_file() for path in screenshots)
    assert all(path.stat().st_size >= 50_000 for path in screenshots)


def test_release_readiness_passes_for_repository():
    report = validate_release(ROOT, expected_version="1.1.0")

    assert report["status"] == "READY"
    assert report["checks_failed"] == 0
    assert report["checks_passed"] == report["checks_total"]


def test_release_readiness_rejects_version_mismatch():
    report = validate_release(ROOT, expected_version="2.0.0")

    assert report["status"] == "BLOCKED"
    assert any(
        check["name"] == "expected_version"
        and check["status"] == "FAIL"
        for check in report["checks"]
    )


def test_release_readiness_cli_check_passes():
    result = subprocess.run(
        [
            sys.executable,
            "src/release_readiness.py",
            "--check",
            "--version",
            "1.1.0",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Release readiness: APROVADO" in result.stdout
