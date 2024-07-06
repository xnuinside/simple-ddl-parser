from dataclasses import Field, dataclass, field
from typing import Callable, Dict, List, Optional

from simple_ddl_parser.output.base_data import BaseData


def update_bigquery_output(statement: dict) -> dict:
    if statement.get("schema") or statement.get("sequences"):
        statement["dataset"] = statement["schema"]
        del statement["schema"]
    return statement


def dialects_clean_up(output_mode: str, table_data) -> Dict:
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
    sortkey: Optional[dict] = field(
        default_factory=lambda: {"type": None, "keys": []},
    )
    diststyle: Optional[str] = field(
        default=None,
    )
    distkey: Optional[str] = field(
        default=None,
    )
    encode: Optional[str] = field(
        default=None,
    )

    def add_additional_keys_in_column_redshift(self, column_data: Dict) -> Dict:
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
    engine: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    default_charset: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    auto_increment: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="bigquery")
class BigQuery(Dialect):
    dataset: Optional[str] = field(default=None)
    project: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )

    @staticmethod
    def prepare_ref_statement(ref_statement: Dict):
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
    _with: Optional[dict] = field(
        default=None, metadata={"exclude_if_not_provided": True, "alias": "with"}
    )
    clustered_primary_key: Optional[list] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    on: Optional[str] = field(default=None, metadata={"exclude_if_not_provided": True})
    textimage_on: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    period_for_system_time: Optional[List[str]] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="databricks")
class Databricks(Dialect):
    property_key: Optional[str] = field(default=None)


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
    organize_by: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    index_in: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="postgres")
class PostgreSQL(Dialect):
    # todo: https://www.postgresql.org/docs/current/sql-createtable.html
    partition_by: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    inherits: Optional[dict] = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="oracle")
class Oracle(Dialect):
    # https://oracle-base.com/articles/8i/index-organized-tables

    is_global: Optional[bool] = field(default=False)
    organization_index: Optional[bool] = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )
    storage: Optional[dict] = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )

    def post_process(self) -> None:
        for column in self.get("columns", []):
            column = self.add_additional_oracle_keys_in_column(column)

    @staticmethod
    def add_additional_oracle_keys_in_column(column_data: Dict) -> Dict:
        column_data.update({"encrypt": None})
        return column_data


@dataclass
@dialect(name="hql")
class HQL(Dialect):
    skewed_by: Optional[dict] = field(
        default_factory=dict,
        metadata={"exclude_if_not_provided": True},
    )
    transient: Optional[bool] = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )
    into_buckets: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    clustered_on: Optional[list] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="snowflake")
class Snowflake(Dialect):
    # create external https://docs.snowflake.com/en/sql-reference/sql/create-external-table
    primary_key_enforced: Optional[bool] = field(
        default=None,
    )
    clone: Optional[dict] = field(
        default=None,
    )
    with_tag: Optional[list] = field(
        default_factory=list,
        metadata={"exclude_if_not_provided": True},
    )


dialect_by_name = {
    obj.__d_name__: obj
    for obj in list(globals().values())
    if isinstance(obj, type) and issubclass(obj, Dialect) and obj != Dialect
}


def add_dialects(dialects: List[Dialect]) -> List[str]:
    return [dialect.__d_name__ for dialect in dialects]


@dataclass
class CommonDialectsFieldsMixin(Dialect):
    """base fields & mixed between dialects"""

    temp: Optional[bool] = field(
        default=False, metadata={"output_modes": add_dialects([HQL, Redshift, Oracle])}
    )
    tblproperties: Optional[dict] = field(
        default_factory=dict,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([SparkSQL, HQL, Redshift]),
        },
    )
    stored_as: Optional[str] = field(
        default=None,
        metadata={"output_modes": add_dialects([SparkSQL, HQL, Databricks, Redshift])},
    )

    row_format: Optional[dict] = field(
        default=None,
        metadata={"output_modes": add_dialects([SparkSQL, HQL, Databricks, Redshift])},
    )
    location: Optional[str] = field(
        default=None,
        metadata={
            "output_modes": add_dialects([HQL, SparkSQL, Snowflake, Databricks]),
            "exclude_if_not_provided": True,
        },
    )
    fields_terminated_by: Optional[str] = field(
        default=None,
        metadata={"output_modes": add_dialects([HQL, Databricks])},
    )
    lines_terminated_by: Optional[str] = field(
        default=None, metadata={"output_modes": add_dialects([HQL, Databricks])}
    )
    map_keys_terminated_by: Optional[str] = field(
        default=None, metadata={"output_modes": add_dialects([HQL, Databricks])}
    )
    collection_items_terminated_by: Optional[str] = field(
        default=None, metadata={"output_modes": add_dialects([HQL, Databricks])}
    )
    clustered_by: Optional[list] = field(
        default=None,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([HQL, SparkSQL]),
        },
    )
    options: Optional[list] = field(
        default=None,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([BigQuery, SparkSQL]),
        },
    )
    transient: Optional[bool] = field(
        default=False,
        metadata={
            "output_modes": add_dialects([HQL, Databricks]),
            "exclude_if_not_provided": True,
        },
    )
    external: Optional[bool] = field(
        default=False, metadata={"output_modes": add_dialects([HQL, Snowflake])}
    )
    cluster_by: Optional[list] = field(
        default_factory=list,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([BigQuery, Snowflake]),
        },
    )


dialect_by_name["sql"] = None
