"""DuckPond target class."""

from __future__ import annotations

from singer_sdk.target_base import SQLTarget
from singer_sdk import typing as th

from target_duckpond.sinks import (
    DuckPondSink,
)


class TargetDuckPond(SQLTarget):
    """Sample target for DuckPond."""

    name = "target-duckpond"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="SQLAlchemy connection string",
        ),
    ).to_dict()

    default_sink_class = DuckPondSink


if __name__ == "__main__":
    TargetDuckPond.cli()
