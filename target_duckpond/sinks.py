"""DuckPond target sink class, which handles writing streams."""

from __future__ import annotations

from textwrap import dedent

import sqlalchemy
from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import SQLSink
from sqlalchemy.sql import Executable


class DuckPondConnector(SQLConnector):
    """The connector for DuckPond.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = False  # Whether temp tables are supported.

    # When defining an Integer column as a primary key, SQLAlchemy uses the SERIAL datatype
    # DuckDB does not support the SERIAL type
    # See workaround:
    # https://github.com/Mause/duckdb_engine#auto-incrementing-id-columns
    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(schema=schema_name)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}",
            ) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            sql_type = self.to_sql_type(property_jsonschema)
            if is_primary_key and isinstance(sql_type, sqlalchemy.Integer):
                # SERIAL workaround
                pk_seq = sqlalchemy.Sequence(
                    f"{schema_name}_{table_name}_{property_name}_pk_seq"
                )
                columns.append(
                    sqlalchemy.Column(
                        property_name,
                        sql_type,
                        pk_seq,
                        server_default=pk_seq.next_value(),
                        primary_key=is_primary_key,
                    ),
                )
            else:
                columns.append(
                    sqlalchemy.Column(
                        property_name,
                        sql_type,
                        primary_key=is_primary_key,
                    ),
                )

        _ = sqlalchemy.Table(table_name, meta, *columns)
        meta.create_all(self._engine)


class DuckPondSink(SQLSink):
    """DuckPond target sink class."""

    connector_class = DuckPondConnector

    def generate_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> str | Executable:
        """Generate an insert statement for the given records.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.

        Returns:
            An insert statement.
        """
        property_names = list(self.conform_schema(schema)["properties"].keys())
        statement = dedent(
            f"""\
            INSERT INTO {full_table_name}
            ({", ".join([f'"{p}"' for p in property_names])})
            VALUES ({", ".join([f":{name}" for name in property_names])})
            """,  # noqa: S608
        )
        return statement.rstrip()
