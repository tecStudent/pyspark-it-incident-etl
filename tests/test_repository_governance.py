from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_governance_files_exist():
    expected_files = [
        "CONTRIBUTING.md",
        "CODE_OF_CONDUCT.md",
        "LICENSE",
        ".github/pull_request_template.md",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/config.yml",
    ]

    assert all((ROOT / file_path).is_file() for file_path in expected_files)


def test_license_is_mit_and_names_author():
    license_text = read("LICENSE")

    assert license_text.startswith("MIT License")
    assert "Copyright (c) 2026 Pedro Magossi Santos" in license_text
    assert "THE SOFTWARE IS PROVIDED \"AS IS\"" in license_text


def test_contributing_documents_complete_workflow():
    content = read("CONTRIBUTING.md")

    for expected in (
        "git switch main",
        "git pull --ff-only origin main",
        "git switch -c feature/",
        "git commit -m",
        "git push -u origin",
        "GitHub Actions",
        "Create a merge commit",
    ):
        assert expected in content


def test_contributing_requires_validation_and_data_safety():
    content = read("CONTRIBUTING.md")

    assert "python3 -m pytest -q" in content
    assert "git diff --check" in content
    assert "Conventional Commits" in content
    assert "Nunca versione o arquivo XLSX acadêmico completo" in content
    assert "Nunca publique senhas, tokens" in content


def test_pr_template_has_expected_sections():
    content = read(".github/pull_request_template.md")

    for heading in (
        "## Objetivo",
        "## Alterações",
        "## Impacto",
        "## Validações",
        "## Evidências",
        "## Checklist",
    ):
        assert heading in content


def test_pr_template_has_quality_checklist():
    content = read(".github/pull_request_template.md")

    assert "Conventional Commits" in content
    assert "Nenhuma credencial" in content
    assert "Nenhum dado acadêmico bruto" in content
    assert "GitHub Actions está verde" in content


def test_bug_issue_form_has_required_metadata():
    content = read(".github/ISSUE_TEMPLATE/bug_report.yml")

    assert "name: Relatar um problema" in content
    assert "title: \"[Bug]: \"" in content
    assert "id: reproduction" in content
    assert "id: expected" in content
    assert content.count("required: true") >= 5


def test_feature_issue_form_has_required_metadata():
    content = read(".github/ISSUE_TEMPLATE/feature_request.yml")

    assert "name: Propor uma melhoria" in content
    assert "title: \"[Melhoria]: \"" in content
    assert "id: problem" in content
    assert "id: acceptance" in content
    assert content.count("required: true") >= 5


def test_issue_template_config_disables_blank_issues():
    content = read(".github/ISSUE_TEMPLATE/config.yml")

    assert "blank_issues_enabled: false" in content
    assert "CONTRIBUTING.md" in content


def test_code_of_conduct_covers_behavior_reporting_and_enforcement():
    content = read("CODE_OF_CONDUCT.md")

    assert "## Comportamentos esperados" in content
    assert "## Comportamentos inaceitáveis" in content
    assert "## Como relatar um problema" in content
    assert "## Aplicação" in content
    assert "@tecStudent" in content


def test_readme_links_governance_files():
    content = read("README.md")

    assert "[CONTRIBUTING.md](CONTRIBUTING.md)" in content
    assert "[Código de Conduta](CODE_OF_CONDUCT.md)" in content
    assert "[licença MIT](LICENSE)" in content
