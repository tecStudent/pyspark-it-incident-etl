from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_security_automation_files_exist():
    expected_files = (
        "SECURITY.md",
        ".github/dependabot.yml",
        ".github/workflows/codeql.yml",
        ".github/workflows/dependency-review.yml",
    )

    assert all((ROOT / path).is_file() for path in expected_files)


def test_security_policy_documents_private_reporting():
    content = read("SECURITY.md")

    assert "## Versões suportadas" in content
    assert "## Relatar uma vulnerabilidade" in content
    assert "Report a vulnerability" in content
    assert "Não abra uma Issue pública" in content
    assert "divulgação responsável" in content


def test_dependabot_covers_python_and_github_actions():
    content = read(".github/dependabot.yml")

    assert "version: 2" in content
    assert "package-ecosystem: pip" in content
    assert "package-ecosystem: github-actions" in content
    assert content.count("directory: /") == 2
    assert content.count("target-branch: main") == 2


def test_dependabot_uses_bounded_weekly_updates():
    content = read(".github/dependabot.yml")

    assert content.count("interval: weekly") == 2
    assert content.count("timezone: America/Sao_Paulo") == 2
    assert content.count("open-pull-requests-limit: 5") == 2
    assert "prefix: deps" in content
    assert "prefix: ci" in content


def test_codeql_runs_for_main_pull_requests_and_schedule():
    content = read(".github/workflows/codeql.yml")

    assert "push:" in content
    assert "pull_request:" in content
    assert content.count("- main") >= 2
    assert "schedule:" in content
    assert "workflow_dispatch:" in content
    assert "pull_request_target:" not in content


def test_codeql_has_least_privilege_permissions():
    content = read(".github/workflows/codeql.yml")

    assert "contents: read" in content
    assert "packages: read" in content
    assert "security-events: write" in content
    assert "actions: write" not in content
    assert "contents: write" not in content


def test_codeql_uses_supported_python_configuration():
    content = read(".github/workflows/codeql.yml")

    assert "actions/checkout@v7" in content
    assert "github/codeql-action/init@v4" in content
    assert "github/codeql-action/analyze@v4" in content
    assert "languages: python" in content
    assert "build-mode: none" in content
    assert "queries: security-extended" in content


def test_dependency_review_blocks_high_risk_additions():
    content = read(".github/workflows/dependency-review.yml")

    assert "pull_request:" in content
    assert "- main" in content
    assert "actions/checkout@v7" in content
    assert "actions/dependency-review-action@v5" in content
    assert "fail-on-severity: high" in content
    assert "vulnerability-check: true" in content
    assert "license-check: true" in content
    assert "pull_request_target:" not in content


def test_readme_exposes_security_status_and_policy():
    content = read("README.md")

    assert "actions/workflows/codeql.yml/badge.svg" in content
    assert "## Segurança automatizada" in content
    assert "[SECURITY.md](SECURITY.md)" in content
    assert "239 passed" in content


def test_security_changes_are_documented_for_contributors():
    contributing = read("CONTRIBUTING.md")
    changelog = read("CHANGELOG.md")

    assert "## Atualizações de dependências" in contributing
    assert "Dependabot" in contributing
    assert "CodeQL" in changelog
    assert "Dependency Review" in changelog
