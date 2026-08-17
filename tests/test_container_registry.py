from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_container_registry_files_exist():
    expected = (
        ".dockerignore",
        ".github/workflows/container-image.yml",
        "docs/container-image.md",
    )

    assert all((ROOT / path).is_file() for path in expected)


def test_dockerignore_uses_a_restrictive_allowlist():
    content = read(".dockerignore")

    assert content.startswith("*\n")
    assert "!src/**" in content
    assert "!data/sample/**" in content
    assert "!data/raw" not in content
    assert "!data/gold" not in content


def test_dockerfile_pins_the_spark_base_image():
    content = read("Dockerfile")

    assert "FROM spark:python3@sha256:" in content


def test_dockerfile_exposes_oci_traceability_labels():
    content = read("Dockerfile")

    assert "ARG VERSION=dev" in content
    assert "ARG REVISION=unknown" in content
    assert "org.opencontainers.image.source" in content
    assert "org.opencontainers.image.version" in content
    assert "org.opencontainers.image.revision" in content


def test_dockerfile_packages_only_required_runtime_content():
    content = read("Dockerfile")

    assert "apt-get upgrade --yes --no-install-recommends" in content
    assert "rm -rf /var/lib/apt/lists/*" in content
    assert "COPY --chown=185:0 src/ /app/src/" in content
    assert "COPY --chown=185:0 VERSION /app/VERSION" in content
    assert "COPY --chown=185:0 data/sample/ /app/data/sample/" in content
    assert "USER 185" in content


def test_dockerfile_has_an_incremental_pipeline_default_command():
    content = read("Dockerfile")

    assert 'CMD ["python3", "src/incremental_pipeline.py"]' in content


def test_workflow_covers_pull_requests_main_tags_and_manual_runs():
    content = read(".github/workflows/container-image.yml")

    assert "pull_request:" in content
    assert "push:" in content
    assert '- "v*.*.*"' in content
    assert "workflow_dispatch:" in content
    assert "- main" in content
    assert "paths:" not in content


def test_workflow_uses_least_privilege_and_safe_events():
    content = read(".github/workflows/container-image.yml")

    assert "contents: read" in content
    assert "packages: write" in content
    assert "contents: write" not in content
    assert "pull_request_target:" not in content


def test_workflow_builds_a_local_validation_image_first():
    content = read(".github/workflows/container-image.yml")

    assert "docker/build-push-action@v6" in content
    assert "load: true" in content
    assert "push: false" in content
    assert ":validation" in content
    assert "Validate container runtime" in content


def test_workflow_blocks_high_and_critical_vulnerabilities():
    content = read(".github/workflows/container-image.yml")

    assert content.count("aquasecurity/trivy-action@v0.36.0") == 2
    assert "Report complete image vulnerabilities" in content
    assert "Enforce actionable image vulnerabilities" in content
    assert 'exit-code: "0"' in content
    assert 'exit-code: "1"' in content
    assert "severity: CRITICAL,HIGH" in content
    assert "ignore-unfixed: true" in content
    assert "skip-dirs: /opt/spark/jars" in content


def test_workflow_publishes_only_for_push_events():
    content = read(".github/workflows/container-image.yml")

    assert content.count("if: github.event_name == 'push'") == 2
    assert "docker/login-action@v3" in content
    assert "docker push" in content


def test_workflow_generates_traceable_version_tags():
    content = read(".github/workflows/container-image.yml")

    assert "docker/metadata-action@v5" in content
    assert "type=sha,prefix=sha-" in content
    assert "type=semver,pattern={{version}}" in content
    assert "type=raw,value=latest,enable={{is_default_branch}}" in content


def test_container_publication_is_documented_and_governed():
    readme = read("README.md")
    changelog = read("CHANGELOG.md")
    ruleset = read("docs/repository-ruleset.md")
    guide = read("docs/container-image.md")

    assert "docs/container-image.md" in readme
    assert "GitHub Container Registry" in changelog
    assert "`Build and scan container image`" in ruleset
    assert "ghcr.io/tecstudent/pyspark-it-incident-etl" in guide
