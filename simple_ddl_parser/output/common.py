import json
import logging
import os
from copy import deepcopy
from dataclasses import Field, dataclass, field
from typing import Any, Dict, Hashable, List, Optional

from simple_ddl_parser.output import dialects as d
from simple_ddl_parser.utils import get_table_id, normalize_name

output_modes = [
    "mssql",
    "mysql",
    "oracle",
    "hql",
    "sql",
    "snowflake",
    "redshift",
    "bigquery",
    "spark_sql",
]


logger = logging.getLogger("simple_ddl_parser")


def dialect(name: str):
    output_modes = {"output_modes": [name]}

    def wrapper(cls):
        cls.__dialect_name__ = name
        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                metadata = value.metadata.copy()
                metadata.update(output_modes)
                value.metadata = metadata
                setattr(cls, key, value)
        return cls

    return wrapper


@dataclass
@dialect(name="redshift")
class RedshiftFields:
    temp: Optional[bool] = field(default=False, metadata={"output": "modes"})
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

    def post_process_dialect_redshift(self) -> None:
        for column in self.columns:
            column = self.add_additional_keys_in_column_redshift(column)
            if self.encode:
                column["encode"] = column["encode"] or self.encode


""" if self.output_mode == "oracle":
            for column in table_data.get("columns", []):
                column = d.add_additional_oracle_keys_in_column(column)
        elif self.output_mode == "snowflake":
            # can be no columns if it is a create database or create schema
            for column in table_data.get("columns", []):
                column = d.add_additional_snowflake_keys_in_column(column)
        elif self.output_mode == "redshift":
            table_data = d.process_redshift_dialect(table_data)"""


@dataclass
@dialect(name="hql")
class HQLFields:
    external: Optional[bool] = field(default=False, metadata={"output": "modes"})
    skewed_by: Optional[dict] = field(
        default_factory=dict,
        metadata={"exclude_if_not_provided": True},
    )
    stored_as: Optional[str] = field(default=None, metadata={"output": "modes"})


@dataclass
@dialect(name="snowflake")
class SnowflakeFields:
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
    replace: Optional[bool] = field(
        default=None, metadata={"exclude_if_not_provided": True}
    )


@dataclass
class DialectsFields(HQLFields, SnowflakeFields, RedshiftFields):
    comment: Optional[str] = field(
        default=None,
        metadata={
            "output_modes": ["hql", "spark_sql", "snowflake"],
            "exclude_if_not_provided": True,
        },
    )
    # snowflake
    tblproperties: Optional[dict] = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )
    location: Optional[str] = field(
        default=None,
        metadata={
            "output_modes": ["hql", "spark_sql", "snowflake"],
            "exclude_if_not_provided": True,
        },
    )
    like: Optional[dict] = field(
        default_factory=dict,
        metadata={"output_modes": ["hql", "redshift"], "exclude_if_not_provided": True},
    )


@dataclass
class TableData(DialectsFields):
    """representation of table data

    exclude_if_not_provided - mean, exclude in output, if was not in data from parser
    """

    # mandatory fields, should have defaults for inheritance
    table_name: str = None
    # final output field set - dialect
    init_data: dict = field(default=None, metadata={"exclude_always": True})
    # final output field set - dialect
    output_mode: str = field(default=None, metadata={"exclude_always": True})
    # optional fields
    schema: Optional[str] = None
    primary_key: Optional[List[str]] = None
    columns: Optional[List[dict]] = field(default_factory=list)
    alter: Optional[Dict] = field(default_factory=dict)
    checks: Optional[List] = field(default_factory=list)
    index: Optional[List] = field(default_factory=list)
    partitioned_by: Optional[List] = field(default_factory=list)
    constraints: Optional[Dict] = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )
    tablespace: Optional[str] = None
    unique: Optional[list] = field(
        default_factory=list, metadata={"exclude_always": True}
    )
    ref_columns: Optional[List[Dict]] = field(
        default_factory=list, metadata={"exclude_always": True}
    )
    references: Optional[List[Dict]] = field(
        default_factory=list, metadata={"exclude_always": True}
    )
    if_not_exists: Optional[bool] = field(
        default=False, metadata={"exclude_if_not_provided": True}
    )
    partition_by: Optional[dict] = field(
        default_factory=dict, metadata={"exclude_if_not_provided": True}
    )
    table_properties: Optional[dict] = field(
        default_factory=dict, metadata={"exclude_if_empty": True}
    )

    def get(self, value: Hashable, default: Any = None) -> Any:
        return self.__dict__.get(value, default)

    def update(self, input_dict: dict) -> None:
        self.__dict__.update(input_dict)

    def if_not_exist_update(self, input_dict: dict) -> None:
        for key, value in input_dict.items():
            if key not in self.__dict__:
                self.__dict__[key] = value

    def __iter__(self):
        for x in self.__dict__:
            yield x

    def __post_init__(self):
        self.set_unique_columns()
        self.populate_keys()
        self.normalize_ref_columns_in_final_output()
        post_process_dialect = getattr(
            self, f"post_process_dialect_{self.output_mode}", None
        )
        if post_process_dialect:
            post_process_dialect()

    @classmethod
    def init(cls, **kwargs):
        cls_fields = {field for field in cls.__dataclass_fields__}
        table_main_args = {k: v for k, v in kwargs.items() if k in cls_fields}
        table_properties = {k: v for k, v in kwargs.items() if k not in table_main_args}
        init_data = {}
        init_data.update(table_main_args)
        init_data.update(table_properties)
        ret = cls(
            **table_main_args, table_properties=table_properties, init_data=init_data
        )
        return ret

    def set_unique_columns(self) -> None:
        unique_keys = ["unique_statement", "constraints"]

        for key in unique_keys:
            if getattr(self, key, None):
                # get column names from unique constraints & statements
                self.set_column_unique_param(key)

    def set_column_unique_param(self, key: str) -> None:
        for column in self.columns:
            if key == "constraints":
                unique = getattr(self, key, {}).get("unique", [])
                if unique:
                    check_in = unique["columns"]
                else:
                    check_in = []
            else:
                check_in = getattr(self, key, {})
            if column["name"] in check_in:
                column["unique"] = True

    def normalize_ref_columns_in_final_output(self):
        for col_ref in self.ref_columns:
            name = col_ref["name"]
            for column in self.columns:
                if name == column["name"]:
                    del col_ref["name"]
                    column["references"] = col_ref

    def populate_keys(self) -> None:
        """primary_key - list of column names, example: "primary_key": ["data_sync_id", "sync_start"],"""

        if not self.primary_key:
            self.get_pk_from_columns_and_constraints()
        else:
            self.remove_pk_from_columns()

        if self.unique:
            self.add_unique_columns()

        for column in self.columns:
            if column["name"] in self.primary_key:
                column["nullable"] = False

    def remove_pk_from_columns(self) -> None:
        for column in self.columns:
            del column["primary_key"]

    def get_pk_from_columns_and_constraints(self) -> None:
        pk = []
        for column in self.columns:
            if column["primary_key"]:
                pk.append(column["name"])
            del column["primary_key"]
        if self.constraints.get("primary_keys"):
            for key_constraints in self.constraints["primary_keys"]:
                pk.extend(key_constraints["columns"])

        self.primary_key = pk

    def add_unique_columns(self) -> None:
        for column in self.columns:
            if column["name"] in self.unique:
                column["unique"] = True

    def filter_out_output(self, field: str) -> bool:
        cls_fields = self.__dataclass_fields__.items()
        exclude_always_keys = set()
        exclude_if_not_provided = set()
        exclude_if_empty = set()
        exclude_by_dialect_filter = set()

        for key, value in cls_fields:
            if value.metadata.get("exclude_always") is True:
                exclude_always_keys.add(key)
            else:
                if value.metadata.get("exclude_if_not_provided") is True:
                    exclude_if_not_provided.add(key)
                if value.metadata.get("exclude_if_empty") is True:
                    exclude_if_empty.add(key)
                if isinstance(
                    value.metadata.get("output_modes"), list
                ) and self.output_mode not in value.metadata.get("output_modes"):
                    exclude_by_dialect_filter.add(key)

        if field in exclude_always_keys:
            return False
        if field in exclude_if_not_provided and field not in self.init_data:
            return False
        if field in exclude_if_empty and not self.get(field):
            return False
        if field in exclude_by_dialect_filter:
            return False
        return True

    def to_dict(self):
        output = {}
        for key, value in self.__dict__.items():
            if self.filter_out_output(key) is True:
                output[key] = value
        output = d.key_cleaning(output, self.output_mode)
        print(output)
        return output


class Output:
    """class implements logic to format final output after parser"""

    def __init__(
        self, parser_output: List[Dict], output_mode: str, group_by_type: bool
    ) -> None:
        self.output_mode = output_mode
        self.group_by_type = group_by_type
        self.parser_output = parser_output

        self.final_result = []
        self.tables_dict = {}

    def get_table_from_tables_data(self, schema: str, table_name: str) -> Dict:
        """get table by name and schema or rise exception"""

        table_id = get_table_id(schema, table_name)
        target_table = self.tables_dict.get(table_id)
        if target_table is None:
            raise ValueError(
                f"TABLE {table_id[0]} with SCHEMA {table_id[1]} does not exists in tables data"
            )
        return target_table

    def clean_up_index_statement(self, statement: Dict) -> None:
        del statement["schema"]
        del statement["table_name"]

        if self.output_mode != "mssql":
            del statement["clustered"]

    def add_index_to_table(self, statement: Dict) -> None:
        """populate 'index' key in output data"""
        target_table = self.get_table_from_tables_data(
            statement["schema"], statement["table_name"]
        )
        self.clean_up_index_statement(statement)
        target_table.index.append(statement)

    def add_alter_to_table(self, statement: Dict) -> None:
        """add 'alter' statement to the table"""
        target_table = self.get_table_from_tables_data(
            statement["schema"], statement["alter_table_name"]
        )

        if "columns" in statement:
            prepare_alter_columns(target_table, statement)
        elif "columns_to_rename" in statement:
            alter_rename_columns(target_table, statement)
        elif "columns_to_drop" in statement:
            alter_drop_columns(target_table, statement)
        elif "columns_to_modify" in statement:
            alter_modify_columns(target_table, statement)
        elif "check" in statement:
            if not target_table.alter.get("checks"):
                target_table.alter["checks"] = []
            statement["check"]["statement"] = " ".join(statement["check"]["statement"])
            target_table.alter["checks"].append(statement["check"])
        elif "unique" in statement:
            target_table = set_alter_to_table_data("unique", statement, target_table)
            target_table = set_unique_columns_from_alter(statement, target_table)
        elif "default" in statement:
            target_table = set_alter_to_table_data("default", statement, target_table)
            target_table = set_default_columns_from_alter(statement, target_table)
        elif "primary_key" in statement:
            target_table = set_alter_to_table_data(
                "primary_key", statement, target_table
            )

    def process_statement_data(self, statement_data: Dict) -> Dict:
        """process tables, types, sequence and etc. data"""

        if statement_data.get("table_name"):
            # mean we have table
            print(statement_data, "statement_data")
            statement_data["output_mode"] = self.output_mode
            table_data = TableData.init(**statement_data)
            self.tables_dict[
                get_table_id(
                    schema_name=table_data.schema,
                    table_name=table_data.table_name,
                )
            ] = table_data
            data = table_data.to_dict()
        else:
            data = statement_data
            d.dialects_clean_up(self.output_mode, data)
        return data

    def process_alter_and_index_result(self, table: Dict):
        if table.get("index_name"):
            self.add_index_to_table(table)

        elif table.get("alter_table_name"):
            self.add_alter_to_table(table)

    def group_by_type_result(self) -> None:
        result_as_dict = {
            "tables": [],
            "types": [],
            "sequences": [],
            "domains": [],
            "schemas": [],
            "ddl_properties": [],
            "comments": [],
        }
        keys_map = {
            "table_name": "tables",
            "sequence_name": "sequences",
            "type_name": "types",
            "domain_name": "domains",
            "schema_name": "schemas",
            "tablespace_name": "tablespaces",
            "database_name": "databases",
            "value": "ddl_properties",
            "comments": "comments",
        }
        for item in self.final_result:
            for key in keys_map:
                if key in item:
                    _type = result_as_dict.get(keys_map.get(key))
                    if _type is None:
                        result_as_dict[keys_map.get(key)] = []
                        _type = result_as_dict[keys_map.get(key)]
                    if key != "comments":
                        _type.append(item)
                    else:
                        _type.extend(item["comments"])
                    break
        if result_as_dict["comments"] == []:
            del result_as_dict["comments"]

        self.final_result = result_as_dict

    def format(self) -> List[Dict]:
        for statement in self.parser_output:
            # process each item in parser output
            if "index_name" in statement or "alter_table_name" in statement:
                self.process_alter_and_index_result(statement)
            else:
                # process tables, types, sequence and etc. data
                statement_data = self.process_statement_data(statement)
                self.final_result.append(statement_data)
        if self.group_by_type:
            self.group_by_type_result()
        return self.final_result


def create_alter_column_references(
    index: int, column: Dict, ref_statement: Dict
) -> Dict:
    """create alter column metadata"""
    column_reference = ref_statement["columns"][index]
    alter_column = {
        "name": column["name"],
        "constraint_name": column.get("constraint_name"),
    }
    alter_column["references"] = deepcopy(ref_statement)
    alter_column["references"]["column"] = column_reference
    del alter_column["references"]["columns"]
    return alter_column


def get_normalized_table_columns_names(target_table: dict) -> List[str]:
    return [normalize_name(column["name"]) for column in target_table.columns]


def prepare_alter_columns(target_table: Dict, statement: Dict) -> Dict:
    """prepare alters column metadata"""
    alter_columns = []
    for num, column in enumerate(statement["columns"]):
        if statement.get("references"):
            alter_columns.append(
                create_alter_column_references(num, column, statement["references"])
            )
        else:
            # mean we need to add
            alter_columns.append(column)
    if not target_table.alter.get("columns"):
        target_table.alter["columns"] = alter_columns
    else:
        target_table.alter["columns"].extend(alter_columns)

    table_columns = get_normalized_table_columns_names(target_table)
    # add columns from 'alter add'
    for column in target_table.alter["columns"]:
        if normalize_name(column["name"]) not in table_columns:
            target_table.columns.append(column)
    return target_table


def set_default_columns_from_alter(statement: Dict, target_table: Dict) -> Dict:
    for column in target_table.columns:
        if statement["default"]["columns"]:
            for column_name in statement["default"]["columns"]:
                if column["name"] == column_name:
                    column["default"] = statement["default"]["value"]
    return target_table


def set_unique_columns_from_alter(statement: Dict, target_table: Dict) -> Dict:
    for column in target_table.columns:
        for column_name in statement["unique"]["columns"]:
            if column["name"] == column_name:
                column["unique"] = True
    return target_table


def alter_modify_columns(target_table, statement) -> None:
    if not target_table.alter.get("modified_columns"):
        target_table.alter["modified_columns"] = []

    for modified_column in statement["columns_to_modify"]:
        index = None
        for num, column in enumerate(target_table.columns):
            if normalize_name(modified_column["name"]) == normalize_name(
                column["name"]
            ):
                index = num
                break
        if index is not None:
            target_table.alter["modified_columns"] = target_table.columns[index]
            target_table.columns[index] = modified_column


def alter_drop_columns(target_table, statement) -> None:
    if not target_table.alter.get("dropped_columns"):
        target_table.alter["dropped_columns"] = []
    for column_to_drop in statement["columns_to_drop"]:
        index = None
        for num, column in enumerate(target_table.columns):
            if normalize_name(column_to_drop) == normalize_name(column["name"]):
                index = num
                break
        if index is not None:
            target_table.alter["dropped_columns"] = target_table.columns[index]
            del target_table.columns[index]


def alter_rename_columns(target_table, statement) -> None:
    for renamed_column in statement["columns_to_rename"]:
        for column in target_table.columns:
            if normalize_name(renamed_column["from"]) == normalize_name(column["name"]):
                column["name"] = renamed_column["to"]
                break

    if not target_table.alter.get("renamed_columns"):
        target_table.alter["renamed_columns"] = []

    target_table.alter["renamed_columns"].extend(statement["columns_to_rename"])


def set_alter_to_table_data(key: str, statement: Dict, target_table: Dict) -> Dict:
    if not target_table.alter.get(key + "s"):
        target_table.alter[key + "s"] = []
    if "using" in statement:
        statement[key]["using"] = statement["using"]
    target_table.alter[key + "s"].append(statement[key])
    return target_table


def dump_data_to_file(table_name: str, dump_path: str, data: List[Dict]) -> None:
    """method to dump json schema"""
    if not os.path.isdir(dump_path):
        os.makedirs(dump_path, exist_ok=True)
    with open("{}/{}_schema.json".format(dump_path, table_name), "w+") as schema_file:
        json.dump(data, schema_file, indent=1)
