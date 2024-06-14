import json
import logging
import os
from typing import Dict, List

from simple_ddl_parser.output.dialects import dialects_clean_up
from simple_ddl_parser.output.table_data import TableData
from simple_ddl_parser.utils import get_table_id

logger = logging.getLogger("simple_ddl_parser")


class Output:
    """class implements logic to format final output after parser"""

    def __init__(
        self, parser_output: List[Dict], output_mode: str, group_by_type: bool
    ) -> None:
        self.output_mode = output_mode
        if output_mode == "bigquery":
            self.schema_key = "dataset"
        else:
            self.schema_key = "schema"
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
        try:
            del statement[self.schema_key]
        except KeyError:
            del statement["schema"]
        del statement["table_name"]

        if self.output_mode != "mssql":
            del statement["clustered"]

    def add_index_to_table(self, statement: Dict) -> None:
        """populate 'index' key in output data"""

        target_table = self.get_table_from_tables_data(
            statement.get(self.schema_key) or statement.get("schema"),
            statement["table_name"],
        )
        self.clean_up_index_statement(statement)
        target_table.index.append(statement)

    def add_alter_to_table(self, statement: Dict) -> None:
        """add 'alter' statement to the table"""
        target_table = self.get_table_from_tables_data(
            statement["schema"], statement["alter_table_name"]
        )
        target_table.append_statement_information_to_table(statement)

    def process_statement_data(self, statement_data: Dict) -> Dict:
        """process tables, types, sequence and etc. data"""

        if statement_data.get("table_name"):
            # mean we have table
            statement_data["output_mode"] = self.output_mode
            table_data = TableData.init(**statement_data)
            self.tables_dict[
                get_table_id(
                    schema_name=getattr(table_data, self.schema_key),
                    table_name=table_data.table_name,
                )
            ] = table_data
            data = table_data.to_dict()
        else:
            data = statement_data
            dialects_clean_up(self.output_mode, data)
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
        if not result_as_dict["comments"]:
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


def dump_data_to_file(table_name: str, dump_path: str, data: List[Dict]) -> None:
    """method to dump json schema"""
    if not os.path.isdir(dump_path):
        os.makedirs(dump_path, exist_ok=True)
    with open("{}/{}_schema.json".format(dump_path, table_name), "w+") as schema_file:
        json.dump(data, schema_file, indent=1)
