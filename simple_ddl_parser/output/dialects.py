from dataclasses import Field, dataclass, field
from typing import Dict, List, Optional


def update_bigquery_output(statement: dict) -> dict:
    if statement.get("schema") or statement.get("sequences"):
        statement["dataset"] = statement["schema"]
        del statement["schema"]
    return statement


def add_additional_snowflake_keys(table_data: Dict) -> Dict:
    table_data.if_not_exist_update({"clone": None, "primary_key_enforced": None})
    return table_data


def add_additional_mssql_keys(table_data: Dict) -> Dict:
    table_data.if_not_exist_update(
        {
            "constraints": {"uniques": None, "checks": None, "references": None},
        }
    )
    return table_data


def clean_up_output(table_data: Dict, key_list: List[str]) -> Dict:
    for key in key_list:
        if key in table_data:
            del table_data[key]
    return table_data


def dialects_clean_up(output_mode: str, table_data) -> Dict:
    print(output_mode)
    update_mappers_for_table_properties = {"bigquery": update_bigquery_output}
    update_table_prop = update_mappers_for_table_properties.get(output_mode)
    if update_table_prop:
        table_data = update_table_prop(table_data)

    return table_data


def dialect(name: Optional[str] = None, names: Optional[List] = None):
    output_modes = {"output_modes": []}
    if name:
        output_modes["output_modes"].append(name)
    if names:
        output_modes["output_modes"].extend(names)

    def wrapper(cls):
        cls.__d_name__ = name
        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                metadata = value.metadata.copy()
                if "output_modes" in metadata:
                    metadata["output_modes"].extend(output_modes["output_modes"])
                else:
                    metadata.update(output_modes)
                value.metadata = metadata
                setattr(cls, key, value)
        return cls

    return wrapper


class DialectMeta(type):
    def __call__(cls, *args, **kwargs):
        output_mode = kwargs.get("output_mode")
        kwargs["dialect"] = dialect_by_name.get(output_mode)
        return super().__call__(*args, **kwargs)


class Dialect(metaclass=DialectMeta):

    """abstract class to implement Dialect"""

    def post_process(self) -> None:
        pass


@dataclass
@dialect(name="redshift")
class Redshift(Dialect):
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
class MySSQL(Dialect):
    pass


@dataclass
@dialect(name="bigquery")
class BigQuery(Dialect):
    dataset: Optional[str] = field(default=False)

    def post_process(self) -> None:
        self.dataset = self.schema
        self.__dataclass_fields__["schema"].metadata = {"exclude_always": True}
        return super().post_process()


@dataclass
@dialect(name="mssql")
class MSSQL(Dialect):
    pass


@dataclass
@dialect(name="databrics")
class Databrics(Dialect):
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
    pass


@dataclass
@dialect(name="postgres")
class PostgreSQL(Dialect):
    # todo: https://www.postgresql.org/docs/current/sql-createtable.html
    partition_by: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )
    inherits: Optional[str] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="oracle")
class Oracle(Dialect):
    def post_process(self) -> None:
        for column in self.get("columns", []):
            column = self.add_additional_oracle_keys_in_column(column)

    def add_additional_oracle_keys_in_column(column_data: Dict) -> Dict:
        column_data.if_not_exist_update({"encrypt": None})
        return column_data


@dataclass
@dialect(name="hql")
class HQL(Dialect):
    external: Optional[bool] = field(default=False)
    skewed_by: Optional[dict] = field(
        default_factory=dict,
        metadata={"exclude_if_not_provided": True},
    )
    transient: Optional[bool] = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(names=["hql", "databrics"])
class HQLDatabrics(Dialect):
    fields_terminated_by: Optional[str] = field(default=None)
    lines_terminated_by: Optional[str] = field(default=None)
    map_keys_terminated_by: Optional[str] = field(default=None)
    collection_items_terminated_by: Optional[str] = field(default=None)
    transient: Optional[bool] = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )


@dataclass
@dialect(name="snowflake")
class Snowflake(Dialect):
    primary_key_enforced: Optional[bool] = field(
        default=None,
    )
    clone: Optional[dict] = field(
        default=None,
    )
    cluster_by: Optional[list] = field(
        default_factory=list,
        metadata={"exclude_if_not_provided": True},
    )
    with_tag: Optional[list] = field(
        default_factory=list,
        metadata={"exclude_if_not_provided": True},
    )


dialect_by_name = {
    obj.__d_name__: obj
    for obj in list(globals().values())
    if isinstance(obj, DialectMeta) and obj != Dialect
}


def add_dialects(dialects: list[Dialect]) -> list[str]:
    print([dialect.__d_name__ for dialect in dialects])
    return [dialect.__d_name__ for dialect in dialects]


@dataclass
class Dialects(*dialect_by_name.values()):
    """base fields & mixed between dialects"""

    temp: Optional[bool] = field(
        default=False, metadata={"output_modes": add_dialects([HQL, Redshift])}
    )
    tblproperties: Optional[dict] = field(
        default_factory=dict,
        metadata={
            "exclude_if_not_provided": True,
            "output_modes": add_dialects([SparkSQL, Redshift]),
        },
    )
    stored_as: Optional[str] = field(
        default=None,
        metadata={"output_modes": add_dialects([SparkSQL, HQL, Databrics, Redshift])},
    )

    row_format: Optional[dict] = field(
        default=None,
        metadata={"output_modes": add_dialects([SparkSQL, HQL, Databrics, Redshift])},
    )
    location: Optional[str] = field(
        default=None,
        metadata={
            "output_modes": add_dialects([HQL, SparkSQL, Snowflake, Databrics]),
            "exclude_if_not_provided": True,
        },
    )

    def post_process(self) -> None:
        # to override dialects post process
        pass


dialect_by_name["sql"] = None
