(function () {
    "use strict";

    const THRESHOLDS = {
        LCP: [2500, 4000],
        CLS: [0.1, 0.25],
        INP: [200, 500],
        TTFB: [800, 1800],
        READY: [3000, 5000]
    };

    const UNITS = {
        LCP: "ms",
        CLS: "",
        INP: "ms",
        TTFB: "ms",
        READY: "ms"
    };

    const metrics = new Map();
    const observers = [];
    const interactions = new Map();
    let clsValue = 0;
    let clsWindowValue = 0;
    let clsWindowStart = 0;
    let clsWindowEnd = 0;


    function ratingFor(name, value) {
        const thresholds = THRESHOLDS[name];

        if (!thresholds || value === null) {
            return "unavailable";
        }

        if (value <= thresholds[0]) {
            return "good";
        }

        if (value <= thresholds[1]) {
            return "needs-improvement";
        }

        return "poor";
    }


    function formatMetric(name, value) {
        if (value === null || value === undefined) {
            return "Indisponível";
        }

        if (name === "CLS") {
            return Number(value).toFixed(3);
        }

        return `${Math.round(value).toLocaleString("pt-BR")} ms`;
    }


    function ratingLabel(rating) {
        return {
            good: "Bom",
            "needs-improvement": "Atenção",
            poor: "Ruim",
            unavailable: "Indisponível"
        }[rating];
    }


    function updateSummary() {
        const summary = document.getElementById("performance-summary");

        if (!summary) {
            return;
        }

        const ratings = Array.from(metrics.values()).map(metric => metric.rating);
        let overall = "good";

        if (ratings.includes("poor")) {
            overall = "poor";
        } else if (ratings.includes("needs-improvement")) {
            overall = "needs-improvement";
        } else if (ratings.length === 0) {
            overall = "unavailable";
        }

        summary.className = `performance-summary rating-${overall}`;
        summary.textContent = ratingLabel(overall);
    }


    function renderMetric(metric) {
        const valueElement = document.getElementById(
            `performance-${metric.name.toLowerCase()}`
        );
        const ratingElement = document.getElementById(
            `performance-${metric.name.toLowerCase()}-rating`
        );

        if (valueElement) {
            valueElement.textContent = formatMetric(metric.name, metric.value);
        }

        if (ratingElement) {
            ratingElement.className = `metric-rating rating-${metric.rating}`;
            ratingElement.textContent = ratingLabel(metric.rating);
        }

        updateSummary();
    }


    function recordMetric(name, value, metadata) {
        const normalizedValue = Number.isFinite(Number(value))
            ? Number(value)
            : null;
        const metric = {
            name,
            value: normalizedValue,
            unit: UNITS[name],
            rating: ratingFor(name, normalizedValue),
            updated_at: new Date().toISOString(),
            metadata: metadata || {}
        };

        metrics.set(name, metric);
        renderMetric(metric);
        document.dispatchEvent(
            new CustomEvent("dashboard:vital", { detail: metric })
        );

        return metric;
    }


    function observe(type, callback, options) {
        if (!("PerformanceObserver" in window)) {
            return false;
        }

        const supportedEntryTypes = (
            PerformanceObserver.supportedEntryTypes || []
        );

        if (!supportedEntryTypes.includes(type)) {
            return false;
        }

        try {
            const observer = new PerformanceObserver(list => {
                callback(list.getEntries());
            });
            observer.observe(options || { type, buffered: true });
            observers.push(observer);
            return true;
        } catch (error) {
            console.debug(`Métrica ${type} indisponível:`, error);
            return false;
        }
    }


    function observeLcp() {
        return observe(
            "largest-contentful-paint",
            entries => {
                const latest = entries.at(-1);

                if (latest) {
                    recordMetric("LCP", latest.startTime, {
                        element: latest.element?.tagName || null
                    });
                }
            },
            { type: "largest-contentful-paint", buffered: true }
        );
    }


    function observeCls() {
        return observe(
            "layout-shift",
            entries => {
                entries.forEach(entry => {
                    if (entry.hadRecentInput) {
                        return;
                    }

                    if (
                        clsWindowStart === 0
                        || entry.startTime - clsWindowEnd > 1000
                        || entry.startTime - clsWindowStart > 5000
                    ) {
                        clsWindowValue = entry.value;
                        clsWindowStart = entry.startTime;
                    } else {
                        clsWindowValue += entry.value;
                    }

                    clsWindowEnd = entry.startTime;
                    clsValue = Math.max(clsValue, clsWindowValue);
                });

                recordMetric("CLS", clsValue);
            },
            { type: "layout-shift", buffered: true }
        );
    }


    function estimatedInp() {
        const values = Array.from(interactions.values()).sort(
            (left, right) => right - left
        );

        if (values.length === 0) {
            return null;
        }

        const percentileIndex = Math.min(
            values.length - 1,
            Math.floor(values.length / 50)
        );
        return values[percentileIndex];
    }


    function observeInp() {
        return observe(
            "event",
            entries => {
                entries.forEach(entry => {
                    if (!entry.interactionId) {
                        return;
                    }

                    interactions.set(
                        entry.interactionId,
                        Math.max(
                            interactions.get(entry.interactionId) || 0,
                            entry.duration
                        )
                    );
                });

                recordMetric("INP", estimatedInp(), {
                    interactions: interactions.size,
                    estimate: true
                });
            },
            {
                type: "event",
                buffered: true,
                durationThreshold: 40
            }
        );
    }


    function measureTtfb() {
        const navigation = performance.getEntriesByType("navigation")[0];

        if (!navigation) {
            recordMetric("TTFB", null);
            return;
        }

        recordMetric("TTFB", navigation.responseStart, {
            transfer_size: navigation.transferSize || 0
        });
    }


    function markDashboardReady(status) {
        requestAnimationFrame(() => {
            recordMetric("READY", performance.now(), {
                status: status || "ready"
            });
        });
    }


    function snapshot() {
        return {
            schema_version: "1.0",
            collected_at: new Date().toISOString(),
            page: window.location.pathname,
            navigation_type: performance.getEntriesByType("navigation")[0]?.type || null,
            metrics: Array.from(metrics.values())
        };
    }


    async function copySnapshot() {
        const status = document.getElementById("performance-copy-status");

        try {
            await navigator.clipboard.writeText(
                JSON.stringify(snapshot(), null, 2)
            );
            if (status) {
                status.textContent = "Diagnóstico copiado.";
            }
        } catch (error) {
            if (status) {
                status.textContent = "Não foi possível copiar automaticamente.";
            }
            console.error("Falha ao copiar diagnóstico:", error);
        }
    }


    function initialize() {
        measureTtfb();

        if (!observeLcp()) {
            recordMetric("LCP", null);
        }
        if (observeCls()) {
            recordMetric("CLS", 0);
        } else {
            recordMetric("CLS", null);
        }
        if (!observeInp()) {
            recordMetric("INP", null);
        }

        document
            .getElementById("copy-performance-diagnostics")
            ?.addEventListener("click", copySnapshot);

    }


    window.dashboardVitals = {
        initialize,
        markDashboardReady,
        snapshot,
        thresholds: THRESHOLDS
    };

    initialize();
}());
