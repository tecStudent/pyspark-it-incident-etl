from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_ruleset_governance_files_exist():
    expected_files = (
        ".github/CODEOWNERS",
        "docs/repository-ruleset.md",
    )

    assert all((ROOT / path).is_file() for path in expected_files)


def test_codeowners_defines_default_owner():
    content = read(".github/CODEOWNERS")

    assert "* @tecStudent" in content


def test_codeowners_protects_critical_areas():
    content = read(".github/CODEOWNERS")

    assert "/.github/ @tecStudent" in content
    assert "/src/ @tecStudent" in content
    assert "/tests/ @tecStudent" in content
    assert "/docs/ @tecStudent" in content
    assert "/requirements.txt @tecStudent" in content


def test_ruleset_targets_active_main_branch():
    content = read("docs/repository-ruleset.md")

    assert "`Protect main`" in content
    assert "**Active**" in content
    assert "**Include default branch**" in content
    assert "`main`" in content


def test_ruleset_requires_pull_requests():
    content = read("docs/repository-ruleset.md")

    assert "**Require a pull request before merging**" in content
    assert "**Require conversation resolution before merging**" in content


def test_ruleset_requires_all_security_checks():
    content = read("docs/repository-ruleset.md")

    assert "`Run PySpark tests`" in content
    assert "`Analyze Python`" in content
    assert "`Review dependency changes`" in content
    assert "**Require status checks to pass**" in content


def test_ruleset_blocks_destructive_branch_operations():
    content = read("docs/repository-ruleset.md")

    assert "**Restrict deletions**" in content
    assert "**Block force pushes**" in content


def test_ruleset_preserves_merge_commit_history():
    content = read("docs/repository-ruleset.md")

    assert "**Create a merge commit**" in content
    assert "Não habilite **Require linear history**" in content


def test_ruleset_avoids_blocking_single_maintainer():
    content = read("docs/repository-ruleset.md")

    assert "**Required approvals** em `0`" in content
    assert "altere **Required approvals** de `0` para `1`" in content
    assert "O autor de um Pull Request não pode aprovar" in content


def test_ruleset_is_linked_from_project_documentation():
    readme = read("README.md")
    contributing = read("CONTRIBUTING.md")
    changelog = read("CHANGELOG.md")

    assert "[Proteção da branch principal](docs/repository-ruleset.md)" in readme
    assert "docs/repository-ruleset.md" in contributing
    assert "CODEOWNERS" in changelog
