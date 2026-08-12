from datetime import datetime, timedelta

from src.forecast_gold import (
    FORECAST_DAYS,
    FORECAST_METHOD,
    FORECAST_METHOD_VERSION,
    HISTORY_DAYS,
    RECENT_AVERAGE_WEIGHT,
    SAME_WEEKDAY_WEIGHT,
    create_forecast_history,
    create_forecast_summary,
)


FORECAST_INPUT_SCHEMA = """
    incident_id string,
    opened_at timestamp,
    priority_code int
"""


def constant_daily_input(spark):
    start = datetime(2025, 1, 1, 10, 0)

    return spark.createDataFrame(
        [
            (
                f"INC{index:03d}",
                start + timedelta(days=index),
                2,
            )
            for index in range(HISTORY_DAYS)
        ],
        schema=FORECAST_INPUT_SCHEMA,
    )


def test_history_completes_missing_calendar_days(spark):
    df = spark.createDataFrame(
        [
            (
                "INC001",
                datetime(2025, 1, 1, 10, 0),
                2,
            ),
            (
                "INC002",
                datetime(2025, 1, 3, 10, 0),
                3,
            ),
            (
                "INC003",
                datetime(2025, 1, 4, 10, 0),
                4,
            ),
        ],
        schema=FORECAST_INPUT_SCHEMA,
    )

    history = create_forecast_history(df)
    rows = history.collect()

    assert len(rows) == HISTORY_DAYS
    assert rows[-1].date.isoformat() == "2025-01-03"
    assert sum(row.actual_incidents for row in rows) == 2
    assert (
        next(
            row
            for row in rows
            if row.date.isoformat() == "2025-01-02"
        ).actual_incidents
        == 0
    )


def test_constant_history_produces_constant_forecast(spark):
    forecast = create_forecast_summary(
        constant_daily_input(spark)
    ).collect()

    assert len(forecast) == FORECAST_DAYS
    assert [row.horizon_day for row in forecast] == list(
        range(1, FORECAST_DAYS + 1)
    )
    assert all(
        row.predicted_incidents == 1
        for row in forecast
    )
    assert all(row.risk_range == 0 for row in forecast)
    assert all(row.lower_bound == 1 for row in forecast)
    assert all(row.upper_bound == 1 for row in forecast)
    assert all(row.forecast_d1 == 1 for row in forecast)
    assert all(row.forecast_d7 == 7 for row in forecast)


def test_forecast_dates_start_after_latest_history(spark):
    forecast = create_forecast_summary(
        constant_daily_input(spark)
    ).collect()

    history_end = datetime(2025, 1, 28).date()

    assert forecast[0].forecast_date == (
        history_end + timedelta(days=1)
    )
    assert forecast[-1].forecast_date == (
        history_end + timedelta(days=7)
    )
    assert all(
        row.history_end_date == history_end
        for row in forecast
    )


def test_weighted_baseline_and_bounds_are_explainable(spark):
    start = datetime(2025, 1, 1, 10, 0)
    data = []
    incident_number = 1

    for day_index in range(HISTORY_DAYS):
        current = start + timedelta(days=day_index)
        daily_volume = 10 if current.weekday() == 6 else 1

        for _ in range(daily_volume):
            data.append(
                (
                    f"INC{incident_number:04d}",
                    current,
                    2,
                )
            )
            incident_number += 1

    df = spark.createDataFrame(
        data,
        schema=FORECAST_INPUT_SCHEMA,
    )

    forecast = create_forecast_summary(df).collect()
    d1 = forecast[0]

    assert d1.predicted_incidents == 2
    assert d1.risk_range > 0
    assert d1.lower_bound == max(
        0,
        d1.predicted_incidents - d1.risk_range,
    )
    assert d1.upper_bound == (
        d1.predicted_incidents + d1.risk_range
    )
    assert d1.method == FORECAST_METHOD
    assert d1.method_version == FORECAST_METHOD_VERSION
    assert d1.same_weekday_weight == SAME_WEEKDAY_WEIGHT
    assert (
        d1.recent_average_weight
        == RECENT_AVERAGE_WEIGHT
    )
    assert d1.scope_priority_codes == "1,2,3"


def test_empty_input_returns_empty_outputs(spark):
    empty_df = spark.createDataFrame(
        [],
        schema=FORECAST_INPUT_SCHEMA,
    )

    assert create_forecast_history(empty_df).count() == 0
    assert create_forecast_summary(empty_df).count() == 0
