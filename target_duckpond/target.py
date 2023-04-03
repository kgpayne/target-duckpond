"""DuckPond target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_duckpond.sinks import DuckPondSink


class TargetDuckPond(SQLTarget):
    """Sample target for DuckPond."""

    name = "target-duckpond"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "pond_root_dir",
            th.StringType,
            description="DuckPond root dir.",
            default="duckpond",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="DuckPond root dir.",
            default="main",
        ),
    ).to_dict()

    default_sink_class = DuckPondSink


if __name__ == "__main__":
    TargetDuckPond.cli()
