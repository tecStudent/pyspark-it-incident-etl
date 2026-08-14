from types import SimpleNamespace

import pytest

from src import gold, incremental_gold, pipeline_benchmark
from src.spark_performance import (
    ANALYTICAL_SPARK_CONFIG,
    cache_reused_frames,
    configure_analytical_builder,
    unpersist_frames,
)


class FakeBuilder:
    def __init__(self):
        self.configs = {}

    def config(self, key, value):
        self.configs[key] = value
        return self


class FakeFrame:
    def __init__(self, name, events=None, fail=False):
        self.name = name
        self.events = events if events is not None else []
        self.fail = fail

    def cache(self):
        self.events.append(("cache", self.name))
        if self.fail:
            raise RuntimeError("cache failure")
        return self

    def unpersist(self):
        self.events.append(("unpersist", self.name))


def test_analytical_profile_matches_approved_benchmark():
    assert ANALYTICAL_SPARK_CONFIG == {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.shuffle.partitions": "8",
    }


def test_benchmark_reuses_production_analytical_profile():
    assert pipeline_benchmark.PROFILE_CONFIGS[
        "optimized"
    ] == ANALYTICAL_SPARK_CONFIG


def test_configure_analytical_builder_applies_every_setting():
    builder = FakeBuilder()

    result = configure_analytical_builder(builder)

    assert result is builder
    assert builder.configs == ANALYTICAL_SPARK_CONFIG


def test_cache_reused_frames_preserves_order():
    events = []
    first = FakeFrame("first", events)
    second = FakeFrame("second", events)

    cached = cache_reused_frames(first, second)

    assert cached == [first, second]
    assert events == [
        ("cache", "first"),
        ("cache", "second"),
    ]


def test_cache_reused_frames_releases_previous_frame_on_failure():
    events = []
    first = FakeFrame("first", events)
    failing = FakeFrame("failing", events, fail=True)

    with pytest.raises(RuntimeError, match="cache failure"):
        cache_reused_frames(first, failing)

    assert events == [
        ("cache", "first"),
        ("cache", "failing"),
        ("unpersist", "first"),
    ]


def test_unpersist_frames_uses_reverse_dependency_order():
    events = []
    first = FakeFrame("first", events)
    second = FakeFrame("second", events)

    unpersist_frames([first, second])

    assert events == [
        ("unpersist", "second"),
        ("unpersist", "first"),
    ]


def test_full_gold_session_uses_analytical_builder(monkeypatch):
    class Session:
        pass

    class Builder:
        def appName(self, name):
            assert name == "IT Incident Gold Aggregations"
            return self

        def getOrCreate(self):
            return Session()

    builder = Builder()
    captured = []
    monkeypatch.setattr(
        gold,
        "SparkSession",
        SimpleNamespace(builder=builder),
    )
    monkeypatch.setattr(
        gold,
        "configure_analytical_builder",
        lambda value: captured.append(value) or value,
    )

    assert isinstance(gold.create_spark_session(), Session)
    assert captured == [builder]


def test_incremental_gold_session_uses_analytical_builder(
    monkeypatch,
):
    class Session:
        pass

    class Builder:
        def appName(self, name):
            assert name == "IT Incident Incremental Gold"
            return self

        def getOrCreate(self):
            return Session()

    builder = Builder()
    captured = []
    monkeypatch.setattr(
        incremental_gold,
        "SparkSession",
        SimpleNamespace(builder=builder),
    )
    monkeypatch.setattr(
        incremental_gold,
        "configure_analytical_builder",
        lambda value: captured.append(value) or value,
    )

    assert isinstance(
        incremental_gold.create_spark_session(),
        Session,
    )
    assert captured == [builder]
