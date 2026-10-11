from collections.abc import Callable
from dataclasses import Field, dataclass, field

from simple_ddl_parser.output.base_data import BaseData


def update_bigquery_output(statement: dict) -> dict:
    if statement.get("schema") or statement.get("sequences"):
        statement["dataset"] = statement["schema"]
        del statement["schema"]
    return statement


def dialects_clean_up(output_mode: str, table_data) -> dict:
    update_mappers_for_table_properties = {"bigquery": update_bigquery_output}
    update_table_prop = update_mappers_for_table_properties.get(output_mode)
    if update_table_prop:
        table_data = update_table_prop(table_data)

    return table_data


def dialect(name: str) -> Callable:
    new_metadata = {"output_modes": [name]}

    def wrapper(cls):
        cls.__d_name__ = name
        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                metadata = value.metadata.copy()
                if "output_modes" in metadata:
                    metadata["output_modes"].extend(new_metadata["output_modes"])
                else:
                    metadata.update(new_metadata)
                value.metadata = metadata
                setattr(cls, key, value)
        return cls

    return wrapper


class Dialect(BaseData):
    """abstract class to implement Dialect"""

    def post_process(self) -> None:
        pass


@dataclass
@dialect(name="redshift")
class Redshift(Dialect):
    # create external https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html
    sortkey: dict | None = field(
        default_factory=lambda: {"type": None, "keys": []},
    )
    diststyle: str | None = field(
        default=None,
    )
    distkey: str | None = field(
        default=None,
    )
    encode: str | None = field(
        default=None,
    )

    def add_additional_keys_in_column_redshift(self, column_data: dict) -> dict:
        column_data["encode"] = column_data.get("encode", None)
        if column_data.get("distkey"):
            self.distkey = column_data["name"]
            del column_data["distkey"]
        return column_data

    def post_process(self) -> None:
        for column in self.columns:
            column = self.add_additional_keys_in_column_redshift(column)
            if self.encode:
                column["encode"] = column["encode"] or self.encode


@dataclass
@dialect(name="spark_sql")
class SparkSQL(Dialect):
    pass


@dataclass
@dialect(name="mysql")
class MySQL(Dialect):
    engine: str | None = field(default=None, metadata={"exclude_if_not_provided": True})
    default_charset: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    auto_increment: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )

    def post_process(self) -> None:
        character = None
        if self.table_properties:
            character = self.table_properties.pop("character", None)
            if character is None:
                character = self.table_properties.pop("charset", None)
        if character and not self.default_charset:
            self.default_charset = character
            if isinstance(self.init_data, dict):
                self.init_data["default_charset"] = character


@dataclass
@dialect(name="bigquery")
class BigQuery(Dialect):
    dataset: str | None = field(default=None)
    project: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )

    @staticmethod
    def prepare_ref_statement(ref_statement: dict):
        ref_statement["dataset"] = ref_statement["schema"]
        del ref_statement["schema"]

    def to_dict(self):
        output = {}
        for key, value in self.__dict__.items():
            if key == "schema":
                continue
            if self.filter_out_output(key) is True:
                name = self.get_alias_if_exists(key)
                output[name] = value
        return output


@dataclass
@dialect(name="mssql")
class MSSQL(Dialect):
    _with: dict | None = field(
        default=None, metadata={"exclude_if_not_provided": True, "alias": "with"}
    )
    clustered_primary_key: list | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    on: str | None = field(default=None, metadata={"exclude_if_not_provided": True})
    textimage_on: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    period_for_system_time: list[str] | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="databricks")
class Databricks(Dialect):
    property_key: str | None = field(default=None)


@dataclass
@dialect(name="sqlite")
class Sqlite(Dialect):
    pass


@dataclass
@dialect(name="vertics")
class Vertica(Dialect):
    pass


@dataclass
@dialect(name="ibm_db2")
class IbmDB2(Dialect):
    organize_by: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    index_in: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="postgres")
class PostgreSQL(Dialect):
    # todo: https://www.postgresql.org/docs/current/sql-createtable.html
    partition_by: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    inherits: dict | None = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="oracle")
class Oracle(Dialect):
    # https://oracle-base.com/articles/8i/index-organized-tables

    is_global: bool | None = field(default=False)
    organization_index: bool | None = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )
    storage: dict | None = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )

    def post_process(self) -> None:
        for column in self.get("columns", []):
            column = self.add_additional_oracle_keys_in_column(column)

    @staticmethod
    def add_additional_oracle_keys_in_column(column_data: dict) -> dict:
        column_data.update({"encrypt": None})
        return column_data


@dataclass
@dialect(name="hql")
class HQL(Dialect):
    skewed_by: dict | None = field(
        default_factory=dict,
        metadata={"exclude_if_not_provided": True},
    )
    transient: bool | None = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )
    into_buckets: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    clustered_on: list | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="snowflake")
class Snowflake(Dialect):
    # create external https://docs.snowflake.com/en/sql-reference/sql/create-external-table
    primary_key_enforced: bool | None = field(
        default=None,
    )
    clone: dict | None = field(
        default=None,
    )
    with_tag: list | None = field(
        default_factory=list,
        metadata={"exclude_if_not_provided": True},
    )
    with_row_access_policy: str | None = field(
        default=None,
        metadata={"exclude_if_not_provided": True},
    )
    # create dynamic table https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table
    dynamic: bool | None = field(
        default=False,
        metadata={"exclude_if_not_provided": True},
    )
    target_lag: str | None = field(
        default=None,
        metadata={"exclude_if_not_provided": True},
    )
    warehouse: str | None = field(
        default=None,
        metadata={"exclude_if_not_provided": True},
    )
    refresh_mode: str | None = field(
        default=None,
        metadata={"exclude_if_not_provided": True},
    )
    initialize: str | None = field(
        default=None,
        metadata={"exclude_if_not_provided": True},
    )
    query: str | None = field(
        default=None,
        metadata={"exclude_if_not_provided": True},
    )
    source_tables: list | None = field(
        default_factory=list,
        metadata={"exclude_if_not_provided": True},
    )


@dataclass
@dialect(name="athena")
class Athena(HQL):
    escaped_by: str | None = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


dialect_by_name = {
    obj.__d_name__: obj
    for obj in list(globals().values())
    if isinstance(obj, type) and issubclass(obj, Dialect) and obj != Dialect
}


def add_dialects(dialects: list[Dialect]) -> list[str]:
    return [dialect.__d_name__ for dialect in dialects]


@dataclass
class CommonDialectsFieldsMixin(Dialect):
    """base fields & mixed between dialects"""

    temp: bool | None = field(
        default=False,
        metadata={"output_modes": add_dialects([HQL, Redshift, Oracle, Athena])},
    )
    tblproperties: dict | None = field(
        default_factory=dict,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([SparkSQL, HQL, Redshift, Athena]),
        },
    )
    stored_as: str | None = field(
        default=None,
        metadata={
            "output_modes": add_dialects([SparkSQL, HQL, Databricks, Redshift, Athena])
        },
    )

    row_format: dict | None = field(
        default=None,
        metadata={
            "output_modes": add_dialects([SparkSQL, HQL, Databricks, Redshift, Athena])
        },
    )
    location: str | None = field(
        default=None,
        metadata={
            "output_modes": add_dialects([HQL, SparkSQL, Snowflake, Databricks]),
            "exclude_if_not_provided": True,
        },
    )
    fields_terminated_by: str | None = field(
        default=None,
        metadata={"output_modes": add_dialects([HQL, Databricks, Athena])},
    )
    lines_terminated_by: str | None = field(
        default=None, metadata={"output_modes": add_dialects([HQL, Databricks, Athena])}
    )
    map_keys_terminated_by: str | None = field(
        default=None, metadata={"output_modes": add_dialects([HQL, Databricks, Athena])}
    )
    collection_items_terminated_by: str | None = field(
        default=None, metadata={"output_modes": add_dialects([HQL, Databricks, Athena])}
    )
    clustered_by: list | None = field(
        default=None,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([HQL, SparkSQL]),
        },
    )
    options: list | None = field(
        default=None,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([BigQuery, SparkSQL]),
        },
    )
    transient: bool | None = field(
        default=False,
        metadata={
            "output_modes": add_dialects([HQL, Databricks, Athena, Snowflake]),
            "exclude_if_not_provided": True,
        },
    )
    external: bool | None = field(
        default=False, metadata={"output_modes": add_dialects([HQL, Snowflake, Athena])}
    )
    cluster_by: list | None = field(
        default_factory=list,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([BigQuery, Snowflake]),
        },
    )

    def __post_init__(self):
        super().__post_init__()
        if (
            getattr(self, "lines_terminated_by", None)
            and self.lines_terminated_by is not None
            and (
                "'\\n'" in self.lines_terminated_by
                or '"\\n"' in self.lines_terminated_by
            )
        ):
            self.lines_terminated_by = self.lines_terminated_by.replace("\\n", "\n")


dialect_by_name["sql"] = None
