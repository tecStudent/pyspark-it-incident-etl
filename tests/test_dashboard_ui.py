from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = ROOT / "docs" / "index.html"
STYLE_PATH = ROOT / "docs" / "css" / "style.css"
SCRIPT_PATH = ROOT / "docs" / "js" / "app.js"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_dashboard_exposes_five_operational_views():
    html = read(INDEX_PATH)

    for view in (
        "overview",
        "trends",
        "forecast",
        "risk",
        "recommendations",
    ):
        assert f'id="{view}-tab"' in html
        assert f'id="{view}-panel"' in html


def test_dashboard_exposes_all_contract_filters():
    html = read(INDEX_PATH)

    for filter_name in (
        "year",
        "month",
        "priority",
        "product",
        "category",
        "team",
    ):
        assert f'id="{filter_name}-filter"' in html


def test_dashboard_loads_all_published_payloads():
    script = read(SCRIPT_PATH)

    for payload in (
        "manifest.json",
        "dashboard_summary.json",
        "filter_options.json",
        "daily_trends.json",
        "risk_summary.json",
        "forecast_summary.json",
        "recommendations.json",
    ):
        assert payload in script


def test_manifest_is_validated_before_operational_payloads_are_rendered():
    script = read(SCRIPT_PATH)

    manifest_fetch = script.index(
        "state.manifest = await fetchJson"
    )
    manifest_validation = script.index(
        "validateManifest(state.manifest)"
    )
    payload_loading = script.index(
        "] = await Promise.all"
    )

    assert manifest_fetch < manifest_validation < payload_loading
    assert 'manifest.status !== "HEALTHY"' in script
    assert "manifest.contains_mock_data" in script


def test_daily_trends_payload_is_loaded_on_demand():
    script = read(SCRIPT_PATH)

    assert "async function ensureTrendsLoaded()" in script
    assert "state.trends = await fetchJson(DATA_PATHS.trends" in script
    assert 'if (view === "trends")' in script
    assert "await ensureTrendsLoaded()" in script


def test_filters_are_enabled_only_for_supported_views():
    script = read(SCRIPT_PATH)

    assert "function updateFilterAvailability(view)" in script
    assert 'overview: new Set(["year", "month", "priority", "team"])' in script
    assert 'trends: new Set(["year", "month", "priority", "product", "category", "team"])' in script
    assert "select.disabled = !enabledFilters.has(filterName)" in script


def test_dashboard_presents_forecast_scope_and_explainability_notice():
    html = read(INDEX_PATH)
    script = read(SCRIPT_PATH)

    assert "Baseline operacional" in html
    assert "não representa um modelo de IA validado" in html
    assert 'setText("forecast-scope", forecast.scope.description' in script


def test_risk_view_displays_methodology_and_excludes_unknown_values():
    html = read(INDEX_PATH)
    script = read(SCRIPT_PATH)

    assert 'id="risk-methodology"' in html
    assert "!item.is_unknown" in script
    assert "state.risk.methodology.weights" in script


def test_recommendations_are_rendered_with_safe_dom_operations():
    script = read(SCRIPT_PATH)

    assert "document.createElement(\"article\")" in script
    assert "recommendation.textContent = item.recommendation" in script
    assert "evidence.textContent" in script
    assert "innerHTML" not in script


def test_dashboard_has_accessible_status_tabs_and_errors():
    html = read(INDEX_PATH)

    assert 'role="status"' in html
    assert 'role="alert"' in html
    assert 'role="tablist"' in html
    assert html.count('role="tab"') == 5
    assert html.count('role="tabpanel"') == 5


def test_dashboard_layout_has_mobile_breakpoints():
    css = read(STYLE_PATH)

    assert "@media (max-width: 820px)" in css
    assert "@media (max-width: 580px)" in css
    assert ".recommendation-list" in css
    assert ".loading-overlay" in css
