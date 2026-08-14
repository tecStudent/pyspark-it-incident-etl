from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = ROOT / "docs" / "index.html"
STYLE_PATH = ROOT / "docs" / "css" / "style.css"
APP_PATH = ROOT / "docs" / "js" / "app.js"
VITALS_PATH = ROOT / "docs" / "js" / "web-vitals.js"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_web_vitals_script_is_loaded_before_dashboard_app():
    html = read(INDEX_PATH)

    assert 'src="js/web-vitals.js"' in html
    assert html.index("js/web-vitals.js") < html.index("js/app.js")


def test_dashboard_exposes_accessible_performance_diagnostics():
    html = read(INDEX_PATH)

    assert 'id="performance-diagnostics"' in html
    assert 'id="performance-copy-status"' in html
    assert 'role="status"' in html
    assert "Nenhum dado é enviado" in html


def test_dashboard_displays_all_measured_metrics():
    html = read(INDEX_PATH)

    for metric in ("lcp", "cls", "inp", "ttfb", "ready"):
        assert f'id="performance-{metric}"' in html
        assert f'id="performance-{metric}-rating"' in html


def test_core_web_vital_thresholds_are_explicit():
    script = read(VITALS_PATH)

    assert "LCP: [2500, 4000]" in script
    assert "CLS: [0.1, 0.25]" in script
    assert "INP: [200, 500]" in script


def test_navigation_and_custom_readiness_thresholds_are_explicit():
    script = read(VITALS_PATH)

    assert "TTFB: [800, 1800]" in script
    assert "READY: [3000, 5000]" in script


def test_performance_observers_use_buffered_browser_entries():
    script = read(VITALS_PATH)

    for entry_type in (
        "largest-contentful-paint",
        "layout-shift",
        '"event"',
    ):
        assert entry_type in script
    assert "buffered: true" in script
    assert "PerformanceObserver.supportedEntryTypes" in script


def test_cls_uses_session_windows_and_ignores_recent_input():
    script = read(VITALS_PATH)

    assert "entry.hadRecentInput" in script
    assert "entry.startTime - clsWindowEnd > 1000" in script
    assert "entry.startTime - clsWindowStart > 5000" in script


def test_inp_estimate_groups_event_entries_by_interaction():
    script = read(VITALS_PATH)

    assert "entry.interactionId" in script
    assert "interactions.set(" in script
    assert "Math.floor(values.length / 50)" in script
    assert "durationThreshold: 40" in script


def test_vitals_snapshot_is_local_and_contains_no_network_sink():
    script = read(VITALS_PATH)

    assert "function snapshot()" in script
    assert "navigator.clipboard.writeText" in script
    assert "sendBeacon" not in script
    assert "fetch(" not in script
    assert "XMLHttpRequest" not in script


def test_dashboard_records_ready_after_loading_overlay_is_hidden():
    script = read(APP_PATH)

    overlay_hidden = script.index("loadingOverlay.hidden = true")
    ready_mark = script.index(
        "window.dashboardVitals?.markDashboardReady"
    )

    assert overlay_hidden < ready_mark
    assert 'dashboardStatus = "error"' in script


def test_metric_ratings_have_visual_states():
    css = read(STYLE_PATH)

    for rating in (
        ".rating-good",
        ".rating-needs-improvement",
        ".rating-poor",
        ".rating-unavailable",
    ):
        assert rating in css


def test_performance_panel_has_responsive_layout():
    css = read(STYLE_PATH)

    assert ".performance-grid" in css
    assert "grid-template-columns: repeat(5" in css
    assert "grid-template-columns: repeat(2" in css
    assert "grid-template-columns: 1fr" in css
