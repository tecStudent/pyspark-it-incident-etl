const DATA_PATHS = {
    manifest: "data/manifest.json",
    overview: "data/dashboard_summary.json",
    filters: "data/filter_options.json",
    trendsIndex: "data/daily_trends_index.json",
    risk: "data/risk_summary.json",
    forecast: "data/forecast_summary.json",
    recommendations: "data/recommendations.json"
};

const COLORS = {
    blue: "#4DA3FF",
    blueSoft: "rgba(77, 163, 255, 0.16)",
    pink: "#ED174F",
    pinkSoft: "rgba(237, 23, 79, 0.15)",
    green: "#00C47A",
    greenSoft: "rgba(0, 196, 122, 0.13)",
    yellow: "#FFC400",
    yellowSoft: "rgba(255, 196, 0, 0.14)",
    muted: "#ADC0CD",
    white: "#F7FBFF",
    grid: "rgba(255, 255, 255, 0.07)"
};

const DIMENSION_LABELS = {
    priority: "Prioridade",
    product: "Produto",
    category: "Categoria",
    assigned_group: "Equipe"
};

const SEVERITY_LABELS = {
    CRITICAL: "Crítica",
    HIGH: "Alta",
    MEDIUM: "Média",
    LOW: "Baixa"
};

const state = {
    manifest: null,
    overview: [],
    filterOptions: null,
    trends: null,
    trendsIndex: null,
    trendPartitions: new Map(),
    trendDefaultApplied: false,
    trendLoadToken: 0,
    risk: null,
    forecast: null,
    recommendations: null,
    activeView: "overview"
};

const charts = {};


function formatNumber(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "-";
    }

    return new Intl.NumberFormat("pt-BR").format(Number(value));
}


function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "-";
    }

    return `${Number(value).toLocaleString("pt-BR", {
        minimumFractionDigits: 1,
        maximumFractionDigits: 2
    })}%`;
}


function formatDate(value) {
    if (!value) {
        return "-";
    }

    const date = new Date(`${value}T00:00:00`);

    return new Intl.DateTimeFormat("pt-BR").format(date);
}


function formatDateTime(value) {
    if (!value) {
        return "-";
    }

    return new Intl.DateTimeFormat("pt-BR", {
        dateStyle: "short",
        timeStyle: "short"
    }).format(new Date(value));
}


function formatDuration(seconds) {
    if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) {
        return "-";
    }

    const hours = Number(seconds) / 3600;

    if (hours >= 72) {
        return `${(hours / 24).toLocaleString("pt-BR", {
            maximumFractionDigits: 1
        })} d`;
    }

    return `${hours.toLocaleString("pt-BR", {
        maximumFractionDigits: 1
    })} h`;
}


function setText(id, value) {
    const element = document.getElementById(id);

    if (element) {
        element.textContent = value;
    }
}


async function fetchJson(path, label) {
    const response = await fetch(path, { cache: "no-store" });

    if (!response.ok) {
        throw new Error(`${label}: resposta HTTP ${response.status}.`);
    }

    try {
        return await response.json();
    } catch (error) {
        throw new Error(`${label}: o arquivo não contém um JSON válido.`, {
            cause: error
        });
    }
}


function validateManifest(manifest) {
    if (manifest.status !== "HEALTHY") {
        throw new Error(`Manifesto com status ${manifest.status || "desconhecido"}.`);
    }

    if (manifest.contains_mock_data) {
        throw new Error("O manifesto informa que existem dados simulados na publicação.");
    }

    const invalidFiles = (manifest.files || []).filter(
        file => file.contract_status !== "VALID" || file.mock
    );

    if (invalidFiles.length > 0 || manifest.files_valid !== manifest.files_total) {
        throw new Error("Nem todos os contratos de dados estão válidos.");
    }
}


function updateManifestStatus() {
    const status = document.getElementById("manifest-status");
    const latestDataTimestamp = (state.manifest.files || [])
        .map(file => file.data_generated_at)
        .filter(Boolean)
        .sort()
        .at(-1);

    status.className = "status-pill status-healthy";
    status.textContent = "Dados íntegros";
    setText("data-updated-at", formatDateTime(latestDataTimestamp));
    setText(
        "contracts-valid",
        `${state.manifest.files_valid}/${state.manifest.files_total}`
    );
    setText("footer-version", `Contrato de dados v${state.manifest.schema_version}`);
}


function showError(error) {
    const banner = document.getElementById("error-banner");
    const status = document.getElementById("manifest-status");

    banner.hidden = false;
    setText("error-message", error.message || String(error));
    status.className = "status-pill status-error";
    status.textContent = "Falha nos dados";
    console.error("Erro ao inicializar dashboard:", error);
}


function addOption(select, value, label) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    select.appendChild(option);
}


function populateFilters(options) {
    const yearSelect = document.getElementById("year-filter");
    const monthSelect = document.getElementById("month-filter");
    const prioritySelect = document.getElementById("priority-filter");
    const productSelect = document.getElementById("product-filter");
    const categorySelect = document.getElementById("category-filter");
    const teamSelect = document.getElementById("team-filter");

    options.years.forEach(year => addOption(yearSelect, year, year));
    options.months.forEach(month => addOption(monthSelect, month.number, month.name));
    options.priorities.forEach(priority => {
        addOption(prioritySelect, priority.code, `${priority.code} - ${priority.name}`);
    });
    options.products.forEach(product => addOption(productSelect, product, product));
    options.categories.forEach(category => addOption(categorySelect, category, category));
    options.teams.forEach(team => addOption(teamSelect, team, team));
}


function selectedFilters() {
    return {
        year: document.getElementById("year-filter").value,
        month: document.getElementById("month-filter").value,
        priority: document.getElementById("priority-filter").value,
        product: document.getElementById("product-filter").value,
        category: document.getElementById("category-filter").value,
        team: document.getElementById("team-filter").value
    };
}


function filteredOverviewRows() {
    const filters = selectedFilters();

    return state.overview.filter(row => {
        return (!filters.year || String(row.opened_year) === filters.year)
            && (!filters.month || String(row.opened_month) === filters.month)
            && (!filters.priority || String(row.priority_code) === filters.priority)
            && (!filters.team || row.assigned_group === filters.team);
    });
}


function filteredTrendRows() {
    if (!state.trends) {
        return [];
    }

    const filters = selectedFilters();

    return state.trends.records.filter(row => {
        const [year, month] = row.date.split("-");

        return (!filters.year || year === filters.year)
            && (!filters.month || Number(month) === Number(filters.month))
            && (!filters.priority || String(row.priority_code) === filters.priority)
            && (!filters.product || row.product === filters.product)
            && (!filters.category || row.category === filters.category)
            && (!filters.team || row.assigned_group === filters.team);
    });
}


function chartOptions({ indexAxis = "x", showLegend = true } = {}) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis,
        interaction: {
            intersect: false,
            mode: "index"
        },
        plugins: {
            legend: {
                display: showLegend,
                labels: {
                    color: COLORS.white,
                    usePointStyle: true,
                    pointStyle: "circle",
                    boxWidth: 7,
                    padding: 18
                }
            },
            tooltip: {
                backgroundColor: "#102330",
                borderColor: "rgba(255,255,255,0.14)",
                borderWidth: 1,
                padding: 12,
                titleColor: COLORS.white,
                bodyColor: COLORS.muted
            }
        },
        scales: {
            x: {
                ticks: { color: COLORS.muted, maxRotation: 45 },
                grid: { color: COLORS.grid }
            },
            y: {
                beginAtZero: true,
                ticks: { color: COLORS.muted },
                grid: { color: COLORS.grid }
            }
        }
    };
}


function drawChart(key, canvasId, configuration) {
    if (charts[key]) {
        charts[key].destroy();
    }

    charts[key] = new Chart(document.getElementById(canvasId), configuration);
}


function aggregateBy(rows, keyBuilder, initialBuilder, accumulator) {
    const result = new Map();

    rows.forEach(row => {
        const key = keyBuilder(row);

        if (!result.has(key)) {
            result.set(key, initialBuilder(row));
        }

        accumulator(result.get(key), row);
    });

    return [...result.values()];
}


function updateOverview() {
    const rows = filteredOverviewRows();
    const totals = rows.reduce(
        (accumulator, row) => {
            accumulator.incidents += row.total_incidents;
            accumulator.kpi += row.kpi_incidents;
            accumulator.violations += row.kpi_violations;
            return accumulator;
        },
        { incidents: 0, kpi: 0, violations: 0 }
    );

    const compliance = totals.kpi > 0
        ? ((totals.kpi - totals.violations) / totals.kpi) * 100
        : null;

    setText("total-incidents", formatNumber(totals.incidents));
    setText("kpi-incidents", formatNumber(totals.kpi));
    setText("kpi-violations", formatNumber(totals.violations));
    setText("kpi-compliance", formatPercent(compliance));

    const monthly = aggregateBy(
        rows,
        row => `${row.opened_year}-${String(row.opened_month).padStart(2, "0")}`,
        row => ({
            key: `${row.opened_year}-${String(row.opened_month).padStart(2, "0")}`,
            total: 0,
            kpi: 0
        }),
        (item, row) => {
            item.total += row.total_incidents;
            item.kpi += row.kpi_incidents;
        }
    ).sort((left, right) => left.key.localeCompare(right.key));

    drawChart("monthly", "monthly-chart", {
        type: "line",
        data: {
            labels: monthly.map(item => {
                const [year, month] = item.key.split("-");
                return `${month}/${year}`;
            }),
            datasets: [
                {
                    label: "Incidentes",
                    data: monthly.map(item => item.total),
                    borderColor: COLORS.yellow,
                    backgroundColor: COLORS.yellowSoft,
                    fill: true,
                    tension: 0.28,
                    pointRadius: 2
                },
                {
                    label: "Incidentes no KPI",
                    data: monthly.map(item => item.kpi),
                    borderColor: COLORS.green,
                    backgroundColor: COLORS.greenSoft,
                    tension: 0.28,
                    pointRadius: 2
                }
            ]
        },
        options: chartOptions()
    });

    const priorities = aggregateBy(
        rows,
        row => row.priority_code,
        row => ({ code: row.priority_code, name: row.priority_name, total: 0 }),
        (item, row) => { item.total += row.total_incidents; }
    ).sort((left, right) => left.code - right.code);

    drawChart("priority", "priority-chart", {
        type: "bar",
        data: {
            labels: priorities.map(item => `${item.code} - ${item.name}`),
            datasets: [{
                label: "Incidentes",
                data: priorities.map(item => item.total),
                backgroundColor: [COLORS.pink, "#ff6c67", COLORS.yellow, COLORS.blue, COLORS.green],
                borderRadius: 6
            }]
        },
        options: chartOptions({ showLegend: false })
    });

    const teams = aggregateBy(
        rows,
        row => row.assigned_group || "Não informado",
        row => ({ name: row.assigned_group || "Não informado", total: 0 }),
        (item, row) => { item.total += row.total_incidents; }
    ).sort((left, right) => right.total - left.total).slice(0, 10);

    drawChart("team", "team-chart", {
        type: "bar",
        data: {
            labels: teams.map(item => item.name),
            datasets: [{
                label: "Incidentes",
                data: teams.map(item => item.total),
                backgroundColor: COLORS.blue,
                borderRadius: 6
            }]
        },
        options: chartOptions({ indexAxis: "y", showLegend: false })
    });
}


function weekStart(dateValue) {
    const date = new Date(`${dateValue}T00:00:00`);
    const day = (date.getDay() + 6) % 7;
    date.setDate(date.getDate() - day);
    return date.toISOString().slice(0, 10);
}


function aggregateTrendRows(rows, keyFunction) {
    return aggregateBy(
        rows,
        keyFunction,
        row => ({
            key: keyFunction(row),
            total: 0,
            kpi: 0,
            violations: 0,
            weightedDuration: 0,
            durationWeight: 0
        }),
        (item, row) => {
            item.total += row.total_incidents;
            item.kpi += row.kpi_incidents;
            item.violations += row.kpi_violations;

            if (row.avg_duration_seconds !== null) {
                item.weightedDuration += row.avg_duration_seconds * row.total_incidents;
                item.durationWeight += row.total_incidents;
            }
        }
    );
}


function trendDimensionValue(row, dimension) {
    if (dimension === "priority") {
        return `${row.priority_code} - ${row.priority_name}`;
    }

    if (dimension === "team") {
        return row.assigned_group || "Não informado";
    }

    return row[dimension] || "Não informado";
}


function updateTrends() {
    if (!state.trends) {
        return;
    }

    const rows = filteredTrendRows();
    const totals = aggregateTrendRows(rows, () => "total")[0] || {
        total: 0,
        kpi: 0,
        violations: 0,
        weightedDuration: 0,
        durationWeight: 0
    };
    const compliance = totals.kpi > 0
        ? ((totals.kpi - totals.violations) / totals.kpi) * 100
        : null;
    const avgDuration = totals.durationWeight > 0
        ? totals.weightedDuration / totals.durationWeight
        : null;

    setText("trend-total", formatNumber(totals.total));
    setText("trend-violations", formatNumber(totals.violations));
    setText("trend-compliance", formatPercent(compliance));
    setText("trend-duration", formatDuration(avgDuration));

    const daily = aggregateTrendRows(rows, row => row.date)
        .sort((left, right) => left.key.localeCompare(right.key));

    drawChart("dailyTrend", "daily-trend-chart", {
        type: "line",
        data: {
            labels: daily.map(item => formatDate(item.key)),
            datasets: [
                {
                    label: "Incidentes",
                    data: daily.map(item => item.total),
                    borderColor: COLORS.blue,
                    backgroundColor: COLORS.blueSoft,
                    fill: true,
                    tension: 0.2,
                    pointRadius: daily.length > 120 ? 0 : 2
                },
                {
                    label: "Violações de KPI",
                    data: daily.map(item => item.violations),
                    borderColor: COLORS.pink,
                    backgroundColor: COLORS.pinkSoft,
                    tension: 0.2,
                    pointRadius: daily.length > 120 ? 0 : 2
                }
            ]
        },
        options: chartOptions()
    });

    const weekly = aggregateTrendRows(rows, row => weekStart(row.date))
        .sort((left, right) => left.key.localeCompare(right.key));

    drawChart("weeklyTrend", "weekly-trend-chart", {
        type: "bar",
        data: {
            labels: weekly.map(item => formatDate(item.key)),
            datasets: [{
                label: "Incidentes",
                data: weekly.map(item => item.total),
                backgroundColor: COLORS.green,
                borderRadius: 4
            }]
        },
        options: chartOptions({ showLegend: false })
    });

    updateDimensionTrend(rows);
}


function updateDimensionTrend(rows = filteredTrendRows()) {
    if (!state.trends) {
        return;
    }

    const dimension = document.getElementById("trend-dimension").value;
    const ranking = aggregateBy(
        rows,
        row => trendDimensionValue(row, dimension),
        row => ({ name: trendDimensionValue(row, dimension), total: 0 }),
        (item, row) => { item.total += row.total_incidents; }
    ).sort((left, right) => right.total - left.total).slice(0, 10);

    drawChart("dimensionTrend", "dimension-trend-chart", {
        type: "bar",
        data: {
            labels: ranking.map(item => item.name),
            datasets: [{
                label: "Incidentes",
                data: ranking.map(item => item.total),
                backgroundColor: COLORS.yellow,
                borderRadius: 5
            }]
        },
        options: chartOptions({ indexAxis: "y", showLegend: false })
    });
}


function updateForecast() {
    const forecast = state.forecast;
    const historyDates = forecast.history.map(item => item.date);
    const forecastDates = forecast.forecast.map(item => item.date);
    const labels = [...historyDates, ...forecastDates];
    const historyPadding = Array(forecastDates.length).fill(null);
    const forecastPadding = Array(historyDates.length).fill(null);

    setText("forecast-d1", formatNumber(forecast.forecast_d1));
    setText("forecast-d7", formatNumber(forecast.forecast_d7));
    setText("forecast-risk-range", forecast.risk_range === null ? "-" : `± ${formatNumber(forecast.risk_range)}`);
    setText("forecast-scope", forecast.scope.description || "Escopo fixo definido pelo pipeline.");

    drawChart("forecast", "forecast-chart", {
        type: "line",
        data: {
            labels: labels.map(formatDate),
            datasets: [
                {
                    label: "Histórico",
                    data: [...forecast.history.map(item => item.actual_incidents), ...historyPadding],
                    borderColor: COLORS.blue,
                    backgroundColor: COLORS.blueSoft,
                    tension: 0.25,
                    pointRadius: 2
                },
                {
                    label: "Previsão",
                    data: [...forecastPadding, ...forecast.forecast.map(item => item.predicted_incidents)],
                    borderColor: COLORS.yellow,
                    backgroundColor: COLORS.yellowSoft,
                    tension: 0.25,
                    pointRadius: 3
                },
                {
                    label: "Limite superior",
                    data: [...forecastPadding, ...forecast.forecast.map(item => item.upper_bound)],
                    borderColor: "rgba(255, 91, 130, 0.72)",
                    borderDash: [5, 5],
                    pointRadius: 0
                },
                {
                    label: "Limite inferior",
                    data: [...forecastPadding, ...forecast.forecast.map(item => item.lower_bound)],
                    borderColor: "rgba(0, 196, 122, 0.72)",
                    borderDash: [5, 5],
                    pointRadius: 0
                }
            ]
        },
        options: chartOptions()
    });

    const tableBody = document.getElementById("forecast-table-body");
    tableBody.replaceChildren();

    forecast.forecast.forEach(item => {
        const row = document.createElement("tr");
        [formatDate(item.date), formatNumber(item.predicted_incidents), formatNumber(item.lower_bound), formatNumber(item.upper_bound)]
            .forEach(value => {
                const cell = document.createElement("td");
                cell.textContent = value;
                row.appendChild(cell);
            });
        tableBody.appendChild(row);
    });
}


function populateRiskDimensions() {
    const select = document.getElementById("risk-dimension");
    const dimensions = [...new Set(state.risk.items.map(item => item.dimension_type))];

    dimensions.forEach(dimension => {
        addOption(select, dimension, DIMENSION_LABELS[dimension] || dimension);
    });

    if (dimensions.includes("assigned_group")) {
        select.value = "assigned_group";
    }
}


function riskColor(score) {
    if (score >= 50) return COLORS.pink;
    if (score >= 25) return "#FF8A3D";
    if (score >= 10) return COLORS.yellow;
    return COLORS.green;
}


function updateRisk() {
    const dimension = document.getElementById("risk-dimension").value;
    const items = state.risk.items
        .filter(item => item.dimension_type === dimension && !item.is_unknown)
        .sort((left, right) => left.rank - right.rank);
    const topItems = items.slice(0, 10);
    const first = topItems[0];
    const weights = state.risk.methodology.weights;

    setText("top-risk-score", first ? first.risk_score.toLocaleString("pt-BR", { maximumFractionDigits: 2 }) : "-");
    setText("top-risk-name", first ? first.dimension_value : "Sem valores conhecidos");
    setText("top-risk-volume", first ? formatNumber(first.volume) : "-");
    setText("risk-items-count", formatNumber(items.length));
    setText(
        "risk-methodology",
        `Pesos: volume ${formatPercent(weights.volume * 100)}, violação ${formatPercent(weights.kpi_violation_rate * 100)} e duração ${formatPercent(weights.avg_duration * 100)}.`
    );

    drawChart("risk", "risk-chart", {
        type: "bar",
        data: {
            labels: topItems.map(item => item.dimension_value),
            datasets: [{
                label: "Score de risco",
                data: topItems.map(item => item.risk_score),
                backgroundColor: topItems.map(item => riskColor(item.risk_score)),
                borderRadius: 5
            }]
        },
        options: {
            ...chartOptions({ indexAxis: "y", showLegend: false }),
            scales: {
                ...chartOptions().scales,
                x: {
                    ...chartOptions().scales.x,
                    beginAtZero: true,
                    max: 100
                }
            }
        }
    });

    const tableBody = document.getElementById("risk-table-body");
    tableBody.replaceChildren();

    topItems.forEach(item => {
        const row = document.createElement("tr");
        const values = [
            item.rank,
            item.dimension_value,
            item.risk_score.toLocaleString("pt-BR", { maximumFractionDigits: 2 }),
            formatNumber(item.volume),
            formatPercent(item.kpi_violation_rate_pct),
            formatDuration(item.avg_duration_seconds)
        ];

        values.forEach((value, index) => {
            const cell = document.createElement("td");
            cell.textContent = value;
            if (index === 2) cell.className = "score-cell";
            row.appendChild(cell);
        });
        tableBody.appendChild(row);
    });
}


function updateRecommendationMetrics() {
    const items = state.recommendations.items;
    setText("critical-recommendations", formatNumber(items.filter(item => item.severity === "CRITICAL").length));
    setText("high-recommendations", formatNumber(items.filter(item => item.severity === "HIGH").length));
    setText("total-recommendations", formatNumber(items.length));
}


function updateRecommendations() {
    const severity = document.getElementById("recommendation-severity").value;
    const items = state.recommendations.items.filter(
        item => !severity || item.severity === severity
    );
    const list = document.getElementById("recommendation-list");
    list.replaceChildren();

    if (items.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.textContent = "Nenhuma recomendação encontrada para a severidade selecionada.";
        list.appendChild(empty);
        return;
    }

    items.forEach((item, index) => {
        const card = document.createElement("article");
        card.className = "recommendation-card";

        const rank = document.createElement("span");
        rank.className = "recommendation-rank";
        rank.textContent = String(index + 1).padStart(2, "0");

        const content = document.createElement("div");
        const meta = document.createElement("div");
        meta.className = "recommendation-meta";

        const severityPill = document.createElement("span");
        severityPill.className = `severity-pill severity-${item.severity.toLowerCase()}`;
        severityPill.textContent = SEVERITY_LABELS[item.severity] || item.severity;

        const target = document.createElement("span");
        target.className = "recommendation-target";
        target.textContent = `${DIMENSION_LABELS[item.dimension_type] || item.dimension_type}: ${item.target || "Não informado"}`;

        const title = document.createElement("h3");
        title.textContent = item.title;

        const recommendation = document.createElement("p");
        recommendation.textContent = item.recommendation;

        const evidence = document.createElement("span");
        evidence.className = "evidence";
        evidence.textContent = `Evidência: ${item.evidence}`;

        meta.append(severityPill, target);
        content.append(meta, title, recommendation, evidence);
        card.append(rank, content);
        list.appendChild(card);
    });
}


function selectedTrendPartitions() {
    const filters = selectedFilters();

    return state.trendsIndex.partitions.filter(partition => {
        return (!filters.year || String(partition.year) === filters.year)
            && (!filters.month || String(partition.month) === filters.month);
    });
}


function applyDefaultTrendPartition() {
    if (state.trendDefaultApplied || !state.trendsIndex.default_partition) {
        return;
    }

    const defaultPartition = state.trendsIndex.default_partition;
    document.getElementById("year-filter").value = String(defaultPartition.year);
    document.getElementById("month-filter").value = String(defaultPartition.month);
    state.trendDefaultApplied = true;
}


async function loadTrendPartitions() {
    const loading = document.getElementById("trends-loading");
    const requestToken = ++state.trendLoadToken;
    const selectedPartitions = selectedTrendPartitions();
    loading.hidden = false;

    try {
        const payloads = await Promise.all(
            selectedPartitions.map(async partition => {
                if (!state.trendPartitions.has(partition.path)) {
                    const payload = await fetchJson(
                        `data/${partition.path}`,
                        `Tendências ${partition.year}-${String(partition.month).padStart(2, "0")}`
                    );

                    if (payload.mock) {
                        throw new Error(`A partição ${partition.path} contém dados simulados.`);
                    }

                    state.trendPartitions.set(partition.path, payload);
                }

                return state.trendPartitions.get(partition.path);
            })
        );

        if (requestToken !== state.trendLoadToken) {
            return;
        }

        state.trends = {
            schema_version: state.trendsIndex.schema_version,
            generated_at: state.trendsIndex.generated_at,
            mock: false,
            records: payloads.flatMap(payload => payload.records)
        };

        updateTrends();
    } catch (error) {
        showError(error);
    } finally {
        if (requestToken === state.trendLoadToken) {
            loading.hidden = true;
        }
    }
}


async function ensureTrendsLoaded() {
    try {
        if (!state.trendsIndex) {
            state.trendsIndex = await fetchJson(
                DATA_PATHS.trendsIndex,
                "Índice das tendências diárias"
            );

            if (state.trendsIndex.mock) {
                throw new Error("O índice de tendências contém dados simulados.");
            }

            applyDefaultTrendPartition();
        }

        await loadTrendPartitions();
    } catch (error) {
        showError(error);
    }
}


function updateFilterScopeNote(view) {
    const messages = {
        overview: "Ano, mês, prioridade e equipe afetam esta visão.",
        trends: "Ano e mês carregam apenas as partições necessárias; os demais filtros refinam o recorte.",
        forecast: "A previsão possui escopo fixo definido pelo pipeline.",
        risk: "O risco é um snapshot consolidado por dimensão.",
        recommendations: "As recomendações são um snapshot consolidado com evidências."
    };

    setText("filter-scope-note", messages[view]);
}


function updateFilterAvailability(view) {
    const filtersByView = {
        overview: new Set(["year", "month", "priority", "team"]),
        trends: new Set(["year", "month", "priority", "product", "category", "team"])
    };
    const enabledFilters = filtersByView[view] || new Set();

    document.querySelectorAll(".filters .filter-select").forEach(select => {
        const filterName = select.id.replace("-filter", "");
        select.disabled = !enabledFilters.has(filterName);
    });
}


async function switchView(view) {
    state.activeView = view;

    document.querySelectorAll(".tab-button").forEach(button => {
        const active = button.dataset.view === view;
        button.classList.toggle("active", active);
        button.setAttribute("aria-selected", String(active));
    });

    document.querySelectorAll(".view-panel").forEach(panel => {
        panel.hidden = panel.id !== `${view}-panel`;
    });

    updateFilterScopeNote(view);
    updateFilterAvailability(view);

    if (view === "trends") {
        await ensureTrendsLoaded();
    } else if (view === "forecast") {
        updateForecast();
    } else if (view === "risk") {
        updateRisk();
    } else if (view === "recommendations") {
        updateRecommendations();
    } else {
        updateOverview();
    }
}


function clearFilters() {
    document.querySelectorAll(".filter-select").forEach(select => {
        if (select.closest(".filters")) {
            select.value = "";
        }
    });

    updateOverview();

    if (state.activeView === "trends" && state.trendsIndex) {
        loadTrendPartitions();
    } else if (state.trends) {
        updateTrends();
    }
}


function registerEvents() {
    document.querySelectorAll(".tab-button").forEach(button => {
        button.addEventListener("click", () => switchView(button.dataset.view));
    });

    document.querySelectorAll(".filters .filter-select").forEach(select => {
        select.addEventListener("change", async () => {
            updateOverview();

            if (state.activeView !== "trends" || !state.trendsIndex) {
                return;
            }

            if (select.id === "year-filter" || select.id === "month-filter") {
                await loadTrendPartitions();
            } else if (state.trends) {
                updateTrends();
            }
        });
    });

    document.getElementById("clear-filters").addEventListener("click", clearFilters);
    document.getElementById("trend-dimension").addEventListener("change", () => updateDimensionTrend());
    document.getElementById("risk-dimension").addEventListener("change", updateRisk);
    document.getElementById("recommendation-severity").addEventListener("change", updateRecommendations);
}


async function loadCoreData() {
    state.manifest = await fetchJson(DATA_PATHS.manifest, "Manifesto de dados");
    validateManifest(state.manifest);

    [
        state.overview,
        state.filterOptions,
        state.risk,
        state.forecast,
        state.recommendations
    ] = await Promise.all([
        fetchJson(DATA_PATHS.overview, "Visão geral"),
        fetchJson(DATA_PATHS.filters, "Opções de filtro"),
        fetchJson(DATA_PATHS.risk, "Ranking de risco"),
        fetchJson(DATA_PATHS.forecast, "Previsão de volume"),
        fetchJson(DATA_PATHS.recommendations, "Recomendações")
    ]);

    const operationalPayloads = [
        state.filterOptions,
        state.risk,
        state.forecast,
        state.recommendations
    ];

    if (operationalPayloads.some(payload => payload.mock)) {
        throw new Error("Um dos payloads carregados contém dados simulados.");
    }
}


async function main() {
    const loadingOverlay = document.getElementById("loading-overlay");

    try {
        if (typeof Chart === "undefined") {
            throw new Error("A biblioteca de gráficos não foi carregada.");
        }

        await loadCoreData();
        updateManifestStatus();
        populateFilters(state.filterOptions);
        populateRiskDimensions();
        updateOverview();
        updateRecommendationMetrics();
        registerEvents();
        updateFilterAvailability("overview");
    } catch (error) {
        showError(error);
    } finally {
        loadingOverlay.hidden = true;
    }
}


main();
