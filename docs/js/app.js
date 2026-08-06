const COLORS = {
    blue: "#4DA3FF",
    pink: "#E50046",
    green: "#00B26B",
    yellow: "#FFC400",
    white: "#FFFFFF"
};

const MONTHS = [
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro"
];

let dashboardData = [];

const charts = {
    monthly: null,
    priority: null,
    team: null
};


function formatNumber(value) {
    return new Intl.NumberFormat("pt-BR").format(value);
}


async function loadData() {
    const response = await fetch(
        "data/dashboard_summary.json"
    );

    if (!response.ok) {
        throw new Error(
            "Erro ao carregar dashboard_summary.json"
        );
    }

    return response.json();
}


function addOption(select, value, label) {
    const option = document.createElement("option");

    option.value = value;
    option.textContent = label;

    select.appendChild(option);
}


function populateFilters(data) {

    const yearSelect =
        document.getElementById("year-filter");

    const monthSelect =
        document.getElementById("month-filter");

    const prioritySelect =
        document.getElementById("priority-filter");

    const teamSelect =
        document.getElementById("team-filter");


    const years = [
        ...new Set(
            data.map(row => row.opened_year)
        )
    ].sort();


    years.forEach(year => {
        addOption(
            yearSelect,
            year,
            year
        );
    });


    const months = [
        ...new Set(
            data.map(row => row.opened_month)
        )
    ].sort((a, b) => a - b);


    months.forEach(month => {
        addOption(
            monthSelect,
            month,
            MONTHS[month - 1]
        );
    });


    const priorities = [
        ...new Map(
            data.map(row => [
                row.priority_code,
                {
                    code: row.priority_code,
                    name: row.priority_name
                }
            ])
        ).values()
    ].sort((a, b) => a.code - b.code);


    priorities.forEach(priority => {
        addOption(
            prioritySelect,
            priority.code,
            `${priority.code} - ${priority.name}`
        );
    });


    const teams = [
        ...new Set(
            data.map(row => row.assigned_group)
        )
    ].sort();


    teams.forEach(team => {
        addOption(
            teamSelect,
            team,
            team
        );
    });
}


function getFilteredData() {

    const year =
        document.getElementById("year-filter").value;

    const month =
        document.getElementById("month-filter").value;

    const priority =
        document.getElementById("priority-filter").value;

    const team =
        document.getElementById("team-filter").value;


    return dashboardData.filter(row => {

        if (
            year &&
            String(row.opened_year) !== year
        ) {
            return false;
        }

        if (
            month &&
            String(row.opened_month) !== month
        ) {
            return false;
        }

        if (
            priority &&
            String(row.priority_code) !== priority
        ) {
            return false;
        }

        if (
            team &&
            row.assigned_group !== team
        ) {
            return false;
        }

        return true;
    });
}


function updateKpis(data) {

    const totals = data.reduce(
        (acc, row) => {

            acc.incidents += row.total_incidents;
            acc.kpi += row.kpi_incidents;
            acc.violations += row.kpi_violations;

            return acc;
        },
        {
            incidents: 0,
            kpi: 0,
            violations: 0
        }
    );


    const compliance =
        totals.kpi > 0
            ? (
                (
                    totals.kpi -
                    totals.violations
                )
                / totals.kpi
            ) * 100
            : null;


    document.getElementById(
        "total-incidents"
    ).textContent =
        formatNumber(totals.incidents);


    document.getElementById(
        "kpi-incidents"
    ).textContent =
        formatNumber(totals.kpi);


    document.getElementById(
        "kpi-violations"
    ).textContent =
        formatNumber(totals.violations);


    document.getElementById(
        "kpi-compliance"
    ).textContent =
        compliance !== null
            ? `${compliance.toFixed(2)}%`
            : "-";
}


function aggregateMonthly(data) {

    const result = new Map();


    data.forEach(row => {

        const key =
            `${row.opened_year}-${row.opened_month}`;


        if (!result.has(key)) {

            result.set(key, {
                year: row.opened_year,
                month: row.opened_month,
                total: 0,
                kpi: 0
            });
        }


        const item = result.get(key);

        item.total += row.total_incidents;
        item.kpi += row.kpi_incidents;
    });


    return [...result.values()].sort(
        (a, b) =>
            a.year - b.year ||
            a.month - b.month
    );
}


function aggregatePriorities(data) {

    const result = new Map();


    data.forEach(row => {

        const key = row.priority_code;


        if (!result.has(key)) {

            result.set(key, {
                code: row.priority_code,
                name: row.priority_name,
                total: 0
            });
        }


        result.get(key).total +=
            row.total_incidents;
    });


    return [...result.values()].sort(
        (a, b) => a.code - b.code
    );
}


function aggregateTeams(data) {

    const result = new Map();


    data.forEach(row => {

        const team = row.assigned_group;

        result.set(
            team,
            (result.get(team) || 0) +
            row.total_incidents
        );
    });


    return [...result.entries()]
        .map(([team, total]) => ({
            team,
            total
        }))
        .sort(
            (a, b) =>
                b.total - a.total
        )
        .slice(0, 10);
}


function chartOptions() {

    return {
        responsive: true,
        maintainAspectRatio: false,

        plugins: {
            legend: {
                labels: {
                    color: COLORS.white
                }
            }
        },

        scales: {

            x: {
                ticks: {
                    color: "#B8C5CF"
                },

                grid: {
                    color:
                        "rgba(255,255,255,0.05)"
                }
            },

            y: {
                beginAtZero: true,

                ticks: {
                    color: "#B8C5CF"
                },

                grid: {
                    color:
                        "rgba(255,255,255,0.05)"
                }
            }
        }
    };
}


function updateMonthlyChart(data) {

    const monthly =
        aggregateMonthly(data);


    if (charts.monthly) {
        charts.monthly.destroy();
    }


    charts.monthly = new Chart(
        document.getElementById(
            "monthlyChart"
        ),
        {
            type: "line",

            data: {

                labels: monthly.map(
                    row =>
                        `${String(row.month).padStart(
                            2,
                            "0"
                        )}/${row.year}`
                ),

                datasets: [
                    {
                        label: "Incidentes",

                        data: monthly.map(
                            row => row.total
                        ),

                        borderColor:
                            COLORS.yellow,

                        backgroundColor:
                            "rgba(255,196,0,0.15)",

                        fill: true,
                        tension: 0.3
                    },

                    {
                        label:
                            "Incidentes no KPI",

                        data: monthly.map(
                            row => row.kpi
                        ),

                        borderColor:
                            COLORS.green,

                        tension: 0.3
                    }
                ]
            },

            options: chartOptions()
        }
    );
}


function updatePriorityChart(data) {

    const priorities =
        aggregatePriorities(data);


    if (charts.priority) {
        charts.priority.destroy();
    }


    charts.priority = new Chart(
        document.getElementById(
            "priorityChart"
        ),
        {
            type: "bar",

            data: {

                labels: priorities.map(
                    row =>
                        `${row.code} - ${row.name}`
                ),

                datasets: [
                    {
                        label: "Incidentes",

                        data: priorities.map(
                            row => row.total
                        ),

                        backgroundColor:
                            COLORS.pink
                    }
                ]
            },

            options: chartOptions()
        }
    );
}


function updateTeamChart(data) {

    const teams =
        aggregateTeams(data);


    if (charts.team) {
        charts.team.destroy();
    }


    charts.team = new Chart(
        document.getElementById(
            "teamChart"
        ),
        {
            type: "bar",

            data: {

                labels: teams.map(
                    row => row.team
                ),

                datasets: [
                    {
                        label: "Incidentes",

                        data: teams.map(
                            row => row.total
                        ),

                        backgroundColor:
                            COLORS.blue
                    }
                ]
            },

            options: {
                ...chartOptions(),
                indexAxis: "y"
            }
        }
    );
}


function updateDashboard() {

    const filteredData =
        getFilteredData();

    updateKpis(filteredData);

    updateMonthlyChart(filteredData);

    updatePriorityChart(filteredData);

    updateTeamChart(filteredData);
}


function clearFilters() {

    document.getElementById(
        "year-filter"
    ).value = "";

    document.getElementById(
        "month-filter"
    ).value = "";

    document.getElementById(
        "priority-filter"
    ).value = "";

    document.getElementById(
        "team-filter"
    ).value = "";

    updateDashboard();
}


async function main() {

    try {

        dashboardData =
            await loadData();

        populateFilters(
            dashboardData
        );


        document
            .querySelectorAll(
                ".filter-select"
            )
            .forEach(select => {

                select.addEventListener(
                    "change",
                    updateDashboard
                );
            });


        document
            .getElementById(
                "clear-filters"
            )
            .addEventListener(
                "click",
                clearFilters
            );


        updateDashboard();

    } catch (error) {

        console.error(
            "Erro ao inicializar dashboard:",
            error
        );
    }
}


main();