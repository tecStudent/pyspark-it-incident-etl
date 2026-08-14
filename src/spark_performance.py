from collections.abc import Iterable
from typing import Any


ANALYTICAL_SPARK_CONFIG = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.shuffle.partitions": "8",
}


def configure_analytical_builder(builder: Any) -> Any:
    """Apply the benchmark-approved local analytical profile."""
    configured_builder = builder

    for key, value in ANALYTICAL_SPARK_CONFIG.items():
        configured_builder = configured_builder.config(
            key,
            value,
        )

    return configured_builder


def cache_reused_frames(*frames: Any) -> list[Any]:
    """Cache lazy DataFrames that are consumed by multiple actions."""
    cached_frames = []

    try:
        for frame in frames:
            cached_frames.append(frame.cache())
    except Exception:
        unpersist_frames(cached_frames)
        raise

    return cached_frames


def unpersist_frames(frames: Iterable[Any]) -> None:
    """Release cached frames in reverse dependency order."""
    for frame in reversed(list(frames)):
        frame.unpersist()
