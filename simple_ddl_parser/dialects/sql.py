import re
from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from simple_ddl_parser.utils import check_spec, remove_par

auth = "AUTHORIZATION"


class AfterColumns:
    @staticmethod
    def _parse_range_bucket(data: List[str]) -> Tuple[List[str], List[str]]:
        range = None

        if len(data) == 3:
            columns = data[0]
            range = data[2]
        else:
            columns = []
            for column in data[0]:
                if "[" in column:
                    range = [column.replace("[", "")]
                elif range:
                    range.append(column.replace("]", ""))
                else:
                    columns.append(column)
        return columns, range

    def p_expression_partition_by(self, p: List) -> None:
        """expr : expr PARTITION BY LP pid RP
        | expr PARTITION BY id LP pid RP
        | expr PARTITION BY pid
        | expr PARTITION BY id pid
        | expr PARTITION BY id LP pid COMMA f_call RP
        """
        p[0] = p[1]
        p_list = remove_par(list(p))
        _type, range, trunc_by = None, None, None

        if isinstance(p_list[4], list):
            columns = p_list[4]
        elif "_TRUNC" in p_list[4]:
            # bigquery
            _type = p_list[4]
            trunc_by = p_list[5][-1]
            p_list[5].pop(-1)
            columns = p_list[5]
        elif p_list[4].upper() == "RANGE_BUCKET":
            # bigquery RANGE_BUCKET with GENERATE_ARRAY
            _type = p_list[4]
            columns, range = self._parse_range_bucket(p_list[5:])
        else:
            columns = p_list[-1]
        if not _type and isinstance(p_list[4], str):
            _type = p_list[4]
        p[0]["partition_by"] = {"columns": columns, "type": _type}
        if range:
            p[0]["partition_by"]["range"] = range
        if trunc_by:
            p[0]["partition_by"]["trunc_by"] = trunc_by


class Database:
    def p_expression_create_database(self, p: List) -> None:
        """expr : expr database_base"""
        p[0] = p[1]
        p_list = list(p)
        p[0].update(p_list[-1])

    def p_database_base(self, p: List) -> None:
        """database_base : CREATE DATABASE id
        | CREATE ID DATABASE id
        | database_base clone
        """
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {}
        p_list = list(p)
        if isinstance(p_list[-1], dict):
            p[0].update(p_list[-1])
        else:
            p[0]["database_name"] = p_list[-1]
        if len(p_list) == 5:
            p[0][p[2].lower()] = True


class TableSpaces:
    @staticmethod
    def get_tablespace_data(p_list):
        if p_list[1] == "TABLESPACE":
            _type = None
            temp = False
        else:
            if p_list[1].upper() == "TEMPORARY":
                _type = None
                temp = True
            else:
                _type = p_list[1]
                if p_list[2].upper() == "TEMPORARY":
                    temp = True
                else:
                    temp = False
        if isinstance(p_list[-1], dict):
            properties = p_list[-1]
            tablespace_name = p_list[-2]
        else:
            properties = None
            tablespace_name = p_list[-1]
        result = {
            "tablespace_name": tablespace_name,
            "properties": properties,
            "type": _type,
            "temporary": temp,
        }
        return result

    def p_expression_create_tablespace(self, p: List) -> None:
        """expr : CREATE TABLESPACE id properties
        | CREATE id TABLESPACE id properties
        | CREATE id TABLESPACE id
        | CREATE TABLESPACE id
        | CREATE id id TABLESPACE id
        | CREATE id id TABLESPACE id properties
        """
        p_list = list(p)
        p[0] = self.get_tablespace_data(p_list[1:])

    def p_properties(self, p: List) -> None:
        """properties : property
        | properties property"""
        p_list = list(p)
        if len(p_list) == 3:
            p[0] = p[1]
            p[0].update(p[2])
        else:
            p[0] = p[1]

    def p_property(self, p: List) -> None:
        """property : id id
        | id STRING
        | id ON
        | id STORAGE
        | IN ROW
        | BY id
        """
        p[0] = {p[1]: p[2]}


class Table:
    @staticmethod
    def add_if_not_exists(data: Dict, p_list: List):
        if "EXISTS" in p_list:
            data["if_not_exists"] = True
        return data

    def p_create_table(self, p: List):
        """create_table : CREATE TABLE IF NOT EXISTS
        | CREATE TABLE
        | CREATE OR REPLACE TABLE IF NOT EXISTS
        | CREATE OR REPLACE TABLE
        | CREATE id TABLE IF NOT EXISTS
        | CREATE id TABLE
        | CREATE id id TABLE
        | CREATE OR REPLACE id TABLE IF NOT EXISTS
        | CREATE OR REPLACE id TABLE

        """
        # id - for EXTERNAL, TRANSIENT, TEMPORARY, GLOBAL, LOCAL, TEMP, VOLATILE, ICEBERG
        # get schema & table name
        p[0] = {}
        p_list = list(p)
        self.add_if_not_exists(p[0], p_list)

        if "REPLACE" in p_list:
            p[0]["replace"] = True
        if "REPLACE" in p_list:
            id_key = p_list[4]
        elif len(p_list) == 5:
            id_key = p_list[3]
        else:
            id_key = p_list[2]
        id_key = id_key.upper()
        if id_key in ["EXTERNAL", "TRANSIENT"]:
            p[0][id_key.lower()] = True
        elif id_key in ["GLOBAL"]:
            p[0]["is_global"] = True
        elif id_key in ["TEMP", "TEMPORARY"]:
            p[0]["temp"] = True
            if len(p_list) == 5 and p_list[2].upper() == "GLOBAL":
                p[0]["is_global"] = True


class Column:
    def p_column_property(self, p: List):
        """c_property : id id"""
        p_list = list(p)
        if p[1].lower() == "auto":
            p[0] = {"increment": True}
        else:
            p[0] = {"property": {p_list[1]: p_list[-1]}}

    def set_base_column_propery(self, p: List) -> Dict:
        if "." in list(p):
            type_str = f"{p[2]}.{p[4]}"
        else:
            type_str = p[2]
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            size = None
            p[0] = {"name": p[1], "type": type_str, "size": size}
        return p[0]

    @staticmethod
    def parse_complex_type(p_list: List[str]) -> str:
        # for complex <> types
        start_index = 1
        _type = ""
        if isinstance(p_list[1], dict):
            _type = p_list[1]["type"]
            start_index = 2

        for elem in p_list[start_index:]:
            if isinstance(elem, list):
                for _elem in elem:
                    _type += f" {_elem.rstrip()}"
            elif "ARRAY" in elem and elem != "ARRAY":
                _type += elem
            else:
                _type += f" {elem}"
        return _type

    def p_c_type(self, p: List) -> None:
        """c_type : id
        | id id
        | id id id id
        | id id id
        | c_type pid
        | id DOT id
        | tid
        | ARRAY
        | c_type ARRAY
        | c_type tid
        """
        p[0] = {}
        p_list = remove_par(list(p))
        _type = None
        if len(p_list) == 2:
            _type = p_list[-1]
        elif isinstance(p[1], str) and p[1].lower() == "encode":
            p[0] = {"property": {"encode": p[2]}}
        else:
            _type = self.parse_complex_type(p_list)
        if _type:
            _type = self.process_type(_type, p_list, p)
        p[0]["type"] = _type

    def process_type(self, _type: Union[str, List], p_list: List, p: List) -> str:
        if isinstance(_type, list):
            _type = _type[0]

        elif isinstance(p_list[-1], str) and p_list[-1].lower() == "distkey":
            p[0] = {"property": {"distkey": True}}
            _type = _type.split("distkey")[0]

        _type = _type.strip().replace(" . ", ".")

        _type = self.process_array_types(_type, p_list)
        return _type

    @staticmethod
    def process_array_types(_type: str, p_list: List) -> str:
        if "<" not in _type and "ARRAY" in _type:
            if "[" not in p_list[-1]:
                _type = _type.replace(" ARRAY", "[]").replace("ARRAY", "[]")
            else:
                _type = _type.replace("ARRAY", "")
        elif "<" in _type and "[]" in _type:
            _type = _type.replace("[]", "ARRAY")
        return _type

    @staticmethod
    def get_size(p_list: List):
        if p_list[-1].isnumeric():
            size = int(p_list[-1])
        else:
            size = p_list[-1]
        if len(p_list) != 3:
            if p_list[-3] != "*":
                # oracle can contain * in column size
                try:
                    value_0 = int(p_list[-3])
                except ValueError:
                    # we have column like p Geometry(MultiPolygon, 26918)
                    value_0 = p_list[-3]
            else:
                value_0 = p_list[-3]
            size = (value_0, int(p_list[-1]))
        return size

    @staticmethod
    def get_column_details(p_list: List, p: List):
        if p_list[-1].get("type"):
            p[0]["type"] += f"{p_list[-1]['type'].strip()}"
        elif p_list[-1].get("comment"):
            p[0].update(p_list[-1])
        elif p_list[-1].get("property"):
            for key, value in p_list[-1]["property"].items():
                p[0][key] = value
        p_list.pop(-1)

    @staticmethod
    def check_type_parameter(size: Union[tuple, int]) -> bool:
        if (
            isinstance(size, tuple)
            and not (isinstance(size[0], str) and size[0].strip() == "*")
            and not (isinstance(size[0], int) or isinstance(size[0], float))
        ):
            return True
        return False

    @staticmethod
    def process_oracle_type_size(p_list):
        if p_list[-1] == ")" and p_list[-4] == "(":
            # for Oracle sizes like 30 CHAR
            p_list[-3] += f" {p_list[-2]}"
            del p_list[-2]
        return p_list

    def process_type_to_column_data(self, p_list, p):
        if "IDENTITY" in p_list[-1]["type"].upper():
            split_type = p_list[-1]["type"].split()
            del p_list[-1]
            if len(split_type) == 1:
                self.set_column_size(p_list, p)
            else:
                p[0]["type"] = split_type[0]
            p[0]["identity"] = None
            return True
        elif len(p_list) <= 3:
            p[0]["type"] = p_list[-1]["type"]
            if p_list[-1].get("property"):
                for key, value in p_list[-1]["property"].items():
                    p[0][key] = value
        else:
            # for [] arrays
            if "[]" in p_list[-1]["type"]:
                p[0]["type"] += p_list[-1]["type"]
            else:
                # types like int UNSIGNED
                p[0]["type"] += f' {p_list[-1]["type"]}'
            del p_list[-1]
        return False

    def p_column(self, p: List) -> None:
        """column : id c_type
        | column comment
        | column LP id RP
        | column LP id id RP
        | column LP id RP c_type
        | column LP id COMMA id RP
        | column LP id COMMA id RP c_type
        """
        if p[1] == "KEY":
            # This is an index
            p[0] = {"index_stmt": True, "name": p[2]["type"], "columns": ""}
            return
        if p[1] and isinstance(p[1], dict) and p[1].get("index_stmt") is True:
            # @TODO: if we are normalizing columns, we need to normalize them here too.
            p[1]["columns"] = remove_par(list(p))[2]
            p[0] = p[1]
            return

        p[0] = self.set_base_column_propery(p)
        identity = False
        p_list = list(p)

        p_list = self.process_oracle_type_size(p_list)

        p_list = remove_par(p_list)
        if isinstance(p_list[-1], dict):
            if "type" in p_list[-1]:
                identity = self.process_type_to_column_data(p_list, p)
            else:
                self.get_column_details(p_list, p)
        if not identity:
            self.set_column_size(p_list, p)

    def set_column_size(self, p_list: List, p: List):
        if (
            not isinstance(p_list[-1], dict)
            and bool(re.match(r"[0-9]+", p_list[-1]))
            or p_list[-1] == "max"
        ):
            size = self.get_size(p_list)
            if self.check_type_parameter(size):
                p[0]["type_parameters"] = size
            elif "identity" in p[0]:
                p[0]["identity"] = size
            else:
                p[0]["size"] = size

    @staticmethod
    def set_property(p: List) -> List:
        for item in p[1:]:
            if isinstance(item, dict):
                if "property" in item:
                    for key, value in item["property"].items():
                        p[0][key] = value
                    del item["property"]
                p[0].update(item)
        return p

    @staticmethod
    def get_column_properties(p_list: List) -> Tuple:
        pk = False
        nullable = True
        default = None
        unique = False
        references = None
        index = False

        if isinstance(p_list[-1], str):
            if p_list[-1].upper() == "KEY":
                if p_list[-2].upper() == "UNIQUE":
                    unique = True
                else:
                    pk = True
                    nullable = False
            if p_list[-1].upper() == "UNIQUE":
                unique = True
        elif isinstance(p_list[-1], dict) and "references" in p_list[-1]:
            p_list[-1]["references"]["column"] = p_list[-1]["references"]["columns"][0]
            del p_list[-1]["references"]["columns"]
            references = p_list[-1]["references"]
        if p_list[-1] == "INDEX":
            index = True
        return pk, default, unique, references, nullable, index

    def p_autoincrement(self, p: List) -> None:
        """autoincrement : AUTOINCREMENT"""
        p[0] = {"autoincrement": True}

    def p_defcolumn(self, p: List) -> None:
        """defcolumn : column
        | defcolumn comment
        | defcolumn encode
        | defcolumn as_virtual
        | defcolumn PRIMARY KEY
        | defcolumn UNIQUE KEY
        | defcolumn UNIQUE
        | defcolumn INDEX
        | defcolumn check_ex
        | defcolumn default
        | defcolumn collate
        | defcolumn enforced
        | defcolumn ref
        | defcolumn null
        | defcolumn ref null
        | defcolumn foreign ref
        | defcolumn encrypt
        | defcolumn generated
        | defcolumn c_property
        | defcolumn on_update
        | defcolumn options
        | defcolumn autoincrement
        | defcolumn option_order_noorder
        | defcolumn option_with_tag
        | defcolumn option_with_masking_policy
        | defcolumn constraint
        | defcolumn generated_by
        | defcolumn timezone
        """
        p[0] = p[1]
        p_list = list(p)

        pk, default, unique, references, nullable, index = self.get_column_properties(
            p_list
        )

        self.set_property(p)

        p[0]["references"] = p[0].get("references", references)
        p[0]["unique"] = unique or p[0].get("unique", unique)
        # @TODO: ensure column names are normalized if specified for pk and others.
        p[0]["primary_key"] = pk or p[0].get("primary_key", pk)
        p[0]["nullable"] = (
            nullable if nullable is not True else p[0].get("nullable", nullable)
        )
        p[0]["default"] = p[0].get("default", default)
        p[0]["check"] = p[0].get("check", None)
        if isinstance(p_list[-1], dict) and p_list[-1].get("encode"):
            p[0]["encode"] = p[0].get("encode", p_list[-1]["encode"])
        if p[0].get("check"):
            if isinstance(p[0].get("check"), dict) or (
                isinstance(p[0].get("check"), list)
                and isinstance(p[0].get("check")[0], dict)
                and p[0].get("check")[0].get("in_statement")
            ):
                check = p[0].get("check")
            else:
                check = self.set_check_in_columm(p[0].get("check"))
            p[0]["check"] = check
        if index:
            p[0]["index"] = index

    @staticmethod
    def set_check_in_columm(check: Optional[List]) -> Optional[str]:
        if check:
            check_statement = ""
            for n, item in enumerate(check):
                if isinstance(item, list):
                    in_clause = ", ".join(item)
                    check_statement += f" ({in_clause})"
                else:
                    if isinstance(item, dict):
                        # mean from id_equals
                        key, value = list(item.items())[0]
                        item = f"{key} = {value}"
                    check_statement += f" {item}" if n > 0 else f"{item}"
            return check_statement

    def p_check_ex(self, p: List) -> None:
        """check_ex : check_st
        | constraint check_st
        """
        name = None
        if isinstance(p[1], dict):
            if "constraint" in p[1]:
                if "in_statement" not in p[2]["check"][0]:
                    statement = " ".join(p[2]["check"])
                else:
                    statement = p[2]["check"][0]
                p[0] = {
                    "check": {
                        "constraint_name": p[1]["constraint"]["name"],
                        "statement": statement,
                    }
                }
            elif "check" in p[1]:
                p[0] = p[1]
                if isinstance(p[1], list):
                    p[0] = {
                        "check": {"constraint_name": name, "statement": p[1]["check"]}
                    }
                if len(p) >= 3:
                    for item in list(p)[2:]:
                        p[0]["check"]["statement"].append(item)
        else:
            p[0] = {"check": {"statement": [p[2]], "constraint_name": name}}


class Schema:
    def p_expression_schema(self, p: List) -> None:
        """expr : create_schema
        | create_database
        | expr id
        | expr clone
        """
        p[0] = p[1]
        p_list = list(p)

        if isinstance(p_list[-1], dict):
            p[0].update(p_list[-1])
        elif len(p) > 2:
            if p[0].get("schema") is not None:
                # then is is a authorization schema property
                p[0]["authorization"] = p[2]
            else:
                if isinstance(p_list[-2], dict):
                    last_key = list(p_list[-2].keys())[-1]
                    p[0][last_key] = p_list[-1]

    def set_properties_for_schema_and_database(self, p: List, p_list: List) -> None:
        if not p[0].get("properties"):
            if len(p_list) == 3:
                properties = p_list[-1]
            elif len(p_list) > 3:
                properties = {p_list[-3]: p_list[-1]}
            else:
                properties = {}
            if properties:
                p[0]["properties"] = properties
        else:
            p[0]["properties"].update({p_list[-3]: p_list[-1]})

    def set_auth_property_in_schema(self, p: List, p_list: List) -> None:
        if p_list[2] == auth:
            p[0] = {"schema_name": p_list[3], auth.lower(): p_list[3]}
        else:
            p[0] = {"schema_name": p_list[2], auth.lower(): p_list[-1]}

    def p_c_schema(self, p: List) -> None:
        """c_schema : CREATE SCHEMA
        | CREATE ID SCHEMA
        | CREATE OR REPLACE SCHEMA"""
        if len(p) == 4:
            p[0] = {"remote": True}

    def p_create_schema(self, p: List) -> None:
        """create_schema : c_schema id id
        | c_schema id id id
        | c_schema id
        | c_schema id DOT id
        | c_schema IF NOT EXISTS id
        | c_schema IF NOT EXISTS id DOT id
        | create_schema options"""
        p_list = list(p)
        p[0] = {}
        auth_index = None

        if "comment" in p_list[-1]:
            del p_list[-1]

        self.add_if_not_exists(p[0], p_list)

        if isinstance(p_list[1], dict):
            p[0] = p_list[1]
            self.set_properties_for_schema_and_database(p, p_list)
        elif auth in p_list:
            auth_index = p_list.index(auth)
            self.set_auth_property_in_schema(p, p_list)

        if isinstance(p_list[-1], str):
            if auth_index:
                schema_name = p_list[auth_index - 1]
                if schema_name is None:
                    schema_name = p_list[auth_index + 1]
            else:
                if "=" in p_list:
                    schema_name = p_list[2]
                else:
                    schema_name = p_list[-1]
            p[0]["schema_name"] = schema_name.replace("`", "")

        p[0] = self.set_project_in_schema(p[0], p_list, auth_index)

    @staticmethod
    def set_project_in_schema(data: Dict, p_list: List, auth_index: int) -> Dict:
        if len(p_list) > 4 and not auth_index and "." in p_list:
            data["project"] = p_list[-3].replace("`", "")
        return data

    def p_create_database(self, p: List) -> None:
        """create_database : database_base
        | create_database multi_id_equals
        | create_database id id STRING
        | create_database options
        """
        p_list = list(p)

        if isinstance(p_list[1], dict):
            p[0] = p_list[1]
            self.set_properties_for_schema_and_database(p, p_list)
        else:
            p[0] = {f"{p[2].lower()}_name": p_list[-1]}


class Drop:
    def p_expression_drop_table(self, p: List) -> None:
        """expr : DROP TABLE id
        | DROP TABLE id DOT id
        """
        # get schema & table name
        p_list = list(p)
        schema = None
        if len(p) > 4:
            if "." in p:
                schema = p_list[-3]
                table_name = p_list[-1]
        else:
            table_name = p_list[-1]
        p[0] = {"schema": schema, "table_name": table_name}


class Type:
    def p_multiple_column_names(self, p: List) -> None:
        """multiple_column_names : column
        | multiple_column_names COMMA
        | multiple_column_names column
        """
        p_list = list(p)
        if isinstance(p[1], dict):
            p[0] = [p[1]]
        else:
            p[0] = p[1]
            if p_list[-1] != ",":
                p[0].append(p_list[-1])

    @staticmethod
    def add_columns_property_for_type(data: Dict, p_list: List) -> Dict:
        if "TABLE" in p_list or isinstance(p_list[-1], dict) and p_list[-1].get("name"):
            if not data["properties"].get("columns"):
                data["properties"]["columns"] = []
            data["properties"]["columns"].append(p_list[-1])
        return data

    @staticmethod
    def set_base_type(data: Dict, p_list: List) -> Dict:
        if len(p_list) > 3:
            data["base_type"] = p_list[2]
        else:
            data["base_type"] = None
        return data

    @staticmethod
    def process_str_base_type(data: Dict, p_list: List) -> Dict:
        base_type = data["base_type"].upper()
        if base_type == "ENUM":
            data["properties"]["values"] = p_list[3]
        elif data["base_type"] == "OBJECT":
            if "type" in p_list[3][0]:
                data["properties"]["attributes"] = p_list[3]
        return data

    def p_type_definition(self, p: List) -> None:  # noqa: C901
        """type_definition : type_name id LP pid RP
        | type_name id LP multiple_column_names RP
        | type_name LP multi_id_equals RP
        | type_name TABLE LP defcolumn
        | type_definition COMMA defcolumn
        | type_definition RP
        """
        p_list = remove_par(list(p))
        p[0] = p[1]
        if not p[0].get("properties"):
            p[0]["properties"] = {}

        p[0] = self.add_columns_property_for_type(p[0], p_list)

        p[0] = self.set_base_type(p[0], p_list)

        if isinstance(p[0]["base_type"], str):
            p[0] = self.process_str_base_type(p[0], p_list)
        elif len(p_list) > 2 and isinstance(p_list[-1], dict):
            p[0]["properties"].update(p_list[-1])

    def p_expression_type_as(self, p: List) -> None:
        """expr : type_definition"""
        p[0] = p[1]

    def p_type_name(self, p: List) -> None:
        """type_name : type_create id AS
        | type_create id DOT id AS
        | type_create id DOT id
        | type_create id
        """
        p_list = list(p)
        p[0] = {}
        if "." not in p_list:
            p[0]["schema"] = None
            p[0]["type_name"] = p_list[2]
        else:
            p[0]["schema"] = p[2]
            p[0]["type_name"] = p_list[4]

    def p_type_create(self, p: List) -> None:
        """type_create : CREATE TYPE
        | CREATE OR REPLACE TYPE
        """
        p[0] = None


class Domain:
    def p_expression_domain_as(self, p: List) -> None:
        """expr : domain_name id LP pid RP"""
        p_list = list(p)
        p[0] = p[1]
        p[0]["base_type"] = p[2]
        p[0]["properties"] = {}
        if p[0]["base_type"] == "ENUM":
            p[0]["properties"]["values"] = p_list[4]

    def p_domain_name(self, p: List) -> None:
        """domain_name : CREATE DOMAIN id AS
        | CREATE DOMAIN id DOT id AS
        | CREATE DOMAIN id DOT id
        | CREATE DOMAIN id
        """
        p_list = list(p)
        p[0] = {}
        if "." not in p_list:
            p[0]["schema"] = None
        else:
            p[0]["schema"] = p[3]
        p[0]["domain_name"] = p_list[-2]


class AlterTable:
    def p_expression_alter(self, p: List) -> None:
        """expr : alter_foreign ref
        | alter_drop_column
        | alter_check
        | alter_unique
        | alter_default
        | alter_primary_key
        | alter_primary_key using_tablespace
        | alter_column_add
        | alter_rename_column
        | alter_column_sql_server
        | alter_column_modify
        | alter_column_modify_oracle
        """
        p[0] = p[1]
        if len(p) == 3:
            p[0].update(p[2])

    def p_alter_column_modify(self, p: List) -> None:
        """alter_column_modify : alt_table MODIFY COLUMN defcolumn"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["columns_to_modify"] = [p_list[-1]]

    def p_alter_drop_column(self, p: List) -> None:
        """alter_drop_column : alt_table DROP COLUMN id"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["columns_to_drop"] = [p_list[-1]]

    def p_alter_rename_column(self, p: List) -> None:
        """alter_rename_column : alt_table RENAME COLUMN id id id"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["columns_to_rename"] = [{"from": p_list[-3], "to": p_list[-1]}]

    def p_alter_column_add(self, p: List) -> None:
        """alter_column_add : alt_table ADD defcolumn"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["columns"] = [p_list[-1]]

    def p_alter_primary_key(self, p: List) -> None:
        """alter_primary_key : alt_table ADD PRIMARY KEY LP pid RP
        | alt_table ADD constraint PRIMARY KEY LP pid RP
        """

        p_list = remove_par(list(p))
        p[0] = p[1]
        p[0]["primary_key"] = {"constraint_name": None, "columns": p_list[-1]}
        if "constraint" in p[3]:
            p[0]["primary_key"]["constraint_name"] = p[3]["constraint"]["name"]

    def p_alter_unique(self, p: List) -> None:
        """alter_unique : alt_table ADD UNIQUE LP pid RP
        | alt_table ADD constraint UNIQUE LP pid RP
        """

        p_list = remove_par(list(p))
        p[0] = p[1]
        p[0]["unique"] = {"constraint_name": None, "columns": p_list[-1]}
        if "constraint" in p[3]:
            p[0]["unique"]["constraint_name"] = p[3]["constraint"]["name"]

    @staticmethod
    def get_column_and_value_from_alter(p: List) -> Tuple:
        p_list = remove_par(list(p))

        column = None
        value = None

        if isinstance(p_list[2], str) and "FOR" == p_list[2].upper():
            column = p_list[-1]
        elif p[0].get("default") and p[0]["default"].get("value"):
            value = p[0]["default"]["value"] + " " + p_list[-1]
        else:
            value = p_list[-1]
        return column, value

    def p_alter_default(self, p: List) -> None:
        """alter_default : alt_table id id
        | alt_table ADD constraint id id
        | alt_table ADD id STRING
        | alt_table ADD constraint id STRING
        | alter_default id
        | alter_default FOR pid
        """
        p[0] = p[1]
        column, value = self.get_column_and_value_from_alter(p)

        if "default" not in p[0]:
            p[0]["default"] = {
                "constraint_name": None,
                "columns": column,
                "value": value,
            }
        else:
            p[0]["default"].update(
                {
                    "columns": p[0]["default"].get("column") or column,
                    "value": value or p[0]["default"].get("value"),
                }
            )
        if "constraint" in p[3]:
            p[0]["default"]["constraint_name"] = p[3]["constraint"]["name"]

    def p_alter_check(self, p: List) -> None:
        """alter_check : alt_table ADD check_ex"""
        p_list = remove_par(list(p))
        p[0] = p[1]
        if isinstance(p[1], dict):
            p[0] = p[1]
        if not p[0].get("check"):
            p[0]["check"] = {"constraint_name": None, "statement": []}
        if "constraint" in p[3]:
            p[0]["check"]["constraint_name"] = p[3]["constraint"]["name"]
        if "constraint_name" in p_list[-1]["check"]:
            p[0]["check"] = p_list[-1]["check"]
        else:
            p[0]["check"]["statement"] = p_list[-1]["check"]

    def p_alter_foreign(self, p: List) -> None:
        """alter_foreign : alt_table ADD foreign
        | alt_table ADD constraint foreign
        """

        p_list = list(p)

        p[0] = p[1]
        if isinstance(p_list[-1], list):
            p[0]["columns"] = [{"name": i} for i in p_list[-1]]
        else:
            column = p_list[-1]

            if not p[0].get("columns"):
                p[0]["columns"] = []
            p[0]["columns"].append(column)

        for column in p[0]["columns"]:
            if isinstance(p_list[3], dict) and "constraint" in p_list[3]:
                column.update({"constraint_name": p_list[3]["constraint"]["name"]})

    def p_alt_table_name(self, p: List) -> None:
        """alt_table : ALTER TABLE t_name
        | ALTER TABLE IF EXISTS t_name
        | ALTER TABLE ID t_name"""
        p_list = list(p)
        table_data = p_list[-1]
        p[0] = {
            "alter_table_name": table_data["table_name"],
            "schema": table_data["schema"],
        }
        if "IF" in p_list:
            p[0]["if_exists"] = True
        if len(p_list) == 6:
            p[0]["only"] = True
        if table_data.get("project"):
            p[0]["project"] = table_data["project"]


class BaseSQL(
    Database,
    Table,
    Drop,
    Domain,
    Column,
    AfterColumns,
    AlterTable,
    Type,
    Schema,
    TableSpaces,
):
    def clean_up_id_list_in_equal(self, p_list: List) -> List:  # noqa R701
        if isinstance(p_list[1], str) and p_list[1].endswith("="):
            p_list[1] = p_list[1][:-1]
        elif "," in p_list:
            if len(p_list) == 4:
                p_list = p_list[-1].split("=")
            elif len(p_list) == 5 and p_list[-2].endswith("="):
                p_list[-2] = p_list[-2][:-1]
        elif "=" == p_list[-2]:
            p_list.pop(-2)
        return p_list

    def get_property(self, p_list: List) -> Dict:
        _property = None
        if not isinstance(p_list[-2], list):
            _value = True
            value = None
            if p_list[-2]:
                if not p_list[-2] == "=":
                    key = p_list[-2]
                else:
                    key = p_list[-3]

            else:
                _value = False
                key = p_list[-1]
            if "=" in key:
                key = key.split("=")
                if _value:
                    value = f"{key[1]} {p_list[-1]}"
                key = key[0]
            else:
                value = p_list[-1]
            _property = {key: value}
        else:
            _property = p_list[-2][0]
        return _property

    def p_multi_id_equals(self, p: List) -> None:
        """multi_id_equals : id_equals
        | multi_id_equals id_equals
        | multi_id_equals COMMA id_equals
        | multi_id_equals COMMA
        """
        p[0] = {}
        for item in list(p)[1:]:
            if item == ",":
                continue
            p[0].update(item)

    def p_id_equals(self, p: List) -> None:
        """id_equals : id EQ id
        | id EQ LP pid RP
        | id EQ ID LP pid RP ID
        | id EQ LP RP
        | id EQ STRING_BASE
        """
        p_list = list(p)

        if not p_list[-1] in [")", "]"]:
            p[0] = {p[1]: p_list[-1]}
        else:
            if len(p_list) > 6 and isinstance(p_list[5], list):
                # pid
                p[0] = {p[1]: p_list[5]}
            elif not p_list[-2] == "(":
                p[0] = {p[1]: p_list[-2]}
            else:
                p[0] = {p[1]: "()"}

    def p_expression_index(self, p: List) -> None:
        """expr : index_table_name LP index_pid RP"""
        p_list = remove_par(list(p))
        p[0] = p[1]
        for item in ["detailed_columns", "columns"]:
            if item not in p[0]:
                p[0][item] = p_list[-1][item]
            else:
                p[0][item].extend(p_list[-1][item])

    def p_index_table_name(self, p: List) -> None:
        """index_table_name : create_index ON id
        | create_index ON id DOT id
        """
        p[0] = p[1]
        p_list = list(p)
        schema = None
        if "." in p_list:
            schema = p_list[-3]
            table_name = p_list[-1]
        else:
            table_name = p_list[-1]
        p[0].update({"schema": schema, "table_name": table_name})

    def p_c_index(self, p: List) -> None:
        """c_index : INDEX LP index_pid RP
        | INDEX id LP index_pid RP
        | c_index INVISIBLE
        | c_index VISIBLE"""
        p_list = remove_par(p_list=list(p))
        if isinstance(p_list[1], dict):
            p[0] = p_list[1]
            p[0]["details"] = {p_list[-1].lower(): True}
        else:
            if len(p_list) == 3:
                name = None
            else:
                name = p_list[2]
            p[0] = {
                "index_stmt": True,
                "name": name,
                "columns": p_list[-1]["detailed_columns"],
            }

    def p_create_index(self, p: List) -> None:
        """create_index : CREATE INDEX id
        | CREATE UNIQUE INDEX id
        | create_index ON id
        | CREATE CLUSTERED INDEX id
        """
        p_list = list(p)
        if "CLUSTERED" in p_list:
            clustered = True
        else:
            clustered = False
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {
                "schema": None,
                "index_name": p_list[-1],
                "unique": "UNIQUE" in p_list,
                "clustered": clustered,
            }

    def extract_check_data(self, p, p_list):
        if isinstance(p_list[-1]["check"], list):
            check = " ".join(p_list[-1]["check"])
            if isinstance(check, str):
                check = {"constraint_name": None, "statement": check}
        else:
            check = p_list[-1]["check"]
            p[0] = self.set_constraint(p[0], "checks", check, check["constraint_name"])
        if not p[0].get("checks"):
            p[0]["checks"] = []
        p[0]["checks"].append(check)
        return p[0]

    def p_expression_table(self, p: List) -> None:  # noqa R701
        """expr : table_name defcolumn
        | table_name LP defcolumn
        | table_name
        | table_name LP RP
        | table_name cluster_by LP defcolumn
        | expr COMMA defcolumn
        | expr COMMA c_index
        | expr COMMA
        | expr COMMA constraint
        | expr COMMA check_ex
        | expr COMMA foreign
        | expr COMMA pkey
        | expr COMMA uniq
        | expr COMMA statem_by_id
        | expr COMMA constraint uniq
        | expr COMMA period_for
        | expr COMMA pkey_constraint
        | expr COMMA constraint pkey
        | expr COMMA constraint pkey enforced
        | expr COMMA constraint foreign ref
        | expr COMMA foreign ref
        | expr encode
        | expr DEFAULT id_equals
        | expr RP
        """
        p[0] = p[1] or defaultdict(list)
        p_list = remove_par(list(p))

        if len(p_list) > 2 and "cluster_by" in p_list[2]:
            p[0].update(p_list[2])
        if p_list[-1] != "," and p_list[-1] is not None:
            if "type" in p_list[-1] and "name" in p_list[-1]:
                if not p[0].get("columns"):
                    p[0]["columns"] = []
                p[0]["columns"].append(p_list[-1])
            elif "index_stmt" in p_list[-1]:
                del p_list[-1]["index_stmt"]
                if not p[0].get("index"):
                    p[0]["index"] = []
                index_data = p_list[-1]
                _index = {
                    "clustered": False,
                    "columns": [index_data["columns"]],
                    "detailed_columns": [
                        {
                            "name": index_data["columns"],
                            "nulls": "LAST",
                            "order": "ASC",
                        }
                    ],
                    "index_name": index_data["name"],
                    "unique": False,
                }
                _index.update(index_data.get("details", {}))
                p[0]["index"].append(_index)
            elif "check" in p_list[-1]:
                p[0] = self.extract_check_data(p, p_list)
            elif "enforced" in p_list[-1]:
                p_list[-2].update(p_list[-1])
                p[0].update({"primary_key_enforced": p_list[-1]["enforced"]})
            elif "DEFAULT" in p_list:
                if isinstance(p_list[-1], dict):
                    value = p_list[-1].get("CHARSET") or p_list[-1].get("charset")
                else:
                    value = p_list[-1]
                p[0].update({"default_charset": value})
            elif isinstance(p_list[-1], dict):
                p[0].update(p_list[-1])

        if isinstance(p_list[-1], dict):
            p[0] = self.process_constraints_and_refs(p[0], p_list)

    def process_unique_and_primary_constraint(self, data: Dict, p_list: List) -> Dict:
        if p_list[-1].get("unique_statement"):
            unique_statement = p_list[-1]["unique_statement"]
            if not isinstance(p_list[-2], dict):
                # This is a stand alone unique statement, not a CONSTRAINT with UNIQUE clause.
                if (
                    isinstance(unique_statement["columns"], list)
                    and len(unique_statement["columns"]) > 1
                ):
                    # We have a list of column names, a compound unique index
                    data = self.set_constraint(
                        data,
                        "uniques",
                        {"columns": unique_statement["columns"]},
                        unique_statement.get(
                            "name", "UC_" + "_".join(unique_statement["columns"])
                        ),
                    )
                else:
                    # We have a single column name.
                    col_name = (
                        unique_statement["columns"][0]
                        if isinstance(unique_statement["columns"], list)
                        else unique_statement["columns"]
                    )
                    for col in data["columns"]:
                        if col["name"] == col_name:
                            col["unique"] = True
            else:
                # We have a constraint specified unique statement.
                data = self.set_constraint(
                    data,
                    "uniques",
                    {"columns": p_list[-1]["unique_statement"]["columns"]},
                    p_list[-2]["constraint"]["name"],
                )
        else:
            data = self.set_constraint(
                data,
                "primary_keys",
                {"columns": p_list[-1]["primary_key"]},
                p_list[-2]["constraint"]["name"],
            )
        return data

    def process_constraints_and_refs(self, data: Dict, p_list: List) -> Dict:
        if "constraint" in p_list[-2] or (
            isinstance(p_list[-1], dict) and p_list[-1].keys() == {"unique_statement"}
        ):
            data = self.process_unique_and_primary_constraint(data, p_list)
        elif (
            len(p_list) >= 4
            and isinstance(p_list[3], dict)
            and p_list[3].get("constraint")
            and p_list[3]["constraint"].get("primary_key")
        ):
            del p_list[3]["constraint"]["primary_key"]
            data = self.set_constraint(
                target_dict=data,
                _type="primary_keys",
                constraint=p_list[3]["constraint"],
                constraint_name=p_list[3]["constraint"]["name"],
            )
            del data["constraint"]
        elif p_list[-1].get("references"):
            data = self.add_ref_information_to_table(data, p_list)
        return data

    def add_ref_information_to_table(self, data, p_list):
        if len(p_list) > 4 and "constraint" in p_list[3]:
            # This is a reference, add the name of the column being referenced
            ref_data = p_list[-1]["references"]
            ref_col_names = p_list[-2]
            if isinstance(ref_col_names, list) and len(ref_col_names) == 1:
                ref_col_names = ref_col_names[0]
            ref_data["name"] = ref_col_names

            data = self.set_constraint(
                data,
                "references",
                ref_data,
                p_list[3]["constraint"]["name"],
            )
        elif isinstance(p_list[-2], list):
            if "ref_columns" not in data:
                data["ref_columns"] = []

            for num, column in enumerate(p_list[-2]):
                ref = deepcopy(p_list[-1]["references"])
                ref["column"] = ref["columns"][num]
                del ref["columns"]
                ref["name"] = column
                data["ref_columns"].append(ref)
        return data

    @staticmethod
    def set_constraint(
        target_dict: Dict, _type: str, constraint: Dict, constraint_name: str
    ) -> Dict:
        if not target_dict.get("constraints"):
            target_dict["constraints"] = {}
        if not target_dict["constraints"].get(_type):
            target_dict["constraints"][_type] = []
        constraint.update({"constraint_name": constraint_name})
        target_dict["constraints"][_type].append(constraint)
        return target_dict

    def p_likke(self, p: List) -> None:
        """likke : LIKE
        | CLONE
        """
        p[0] = p[1].lower()

    def p_expression_like_table(self, p: List) -> None:
        """expr : table_name likke id
        | table_name likke id DOT id
        | table_name LP likke id DOT id RP
        | table_name LP likke id RP
        """
        # get schema & table name
        p_list = remove_par(list(p))
        if len(p_list) > 4:
            if "." in p:
                schema = p_list[-3]
                table_name = p_list[-1]
            key = p_list[-4]
        else:
            table_name = p_list[-1]
            schema = None
            key = p_list[-2]
        p[0] = p[1]
        p[0].update({key: {"schema": schema, "table_name": table_name}})

    def p_t_name(self, p: List) -> None:
        """t_name : id DOT id
        | id
        | id DOT id DOT id
        """
        p_list = list(p)

        project = None

        if len(p) > 3:
            if "." in p:
                schema = p_list[-3]
                table_name = p_list[-1]
                if len(p) == 6:
                    project = p_list[1]
        else:
            table_name = p_list[-1]
            schema = None

        p[0] = {"schema": schema, "table_name": table_name, "columns": [], "checks": []}

        if project:
            p[0]["project"] = project

    def p_table_name(self, p: List) -> None:
        """table_name : create_table t_name
        | table_name likke id
        """
        # can contain additional properties like 'external for HQL
        p[0] = p[1]

        p[0].update(list(p)[-1])

    def p_expression_seq(self, p: List) -> None:
        """expr : seq_name
        | expr INCREMENT id
        | expr INCREMENT BY id
        | expr INCREMENT id id
        | expr START id
        | expr START WITH id
        | expr START id id
        | expr MINVALUE id
        | expr NO MINVALUE
        | expr NO MAXVALUE
        | expr MAXVALUE id
        | expr CACHE id
        | expr CACHE
        | expr NOORDER
        | expr ORDER
        """
        # get schema & table name
        p_list = list(p)
        p[0] = p[1]
        value = None
        if len(p) == 4:
            if p[2] == "NO":
                value = {p_list[-1].lower(): False}
            else:
                value = {p[2].lower(): int(p_list[-1])}
        elif len(p) == 3:
            value = {p[2].lower(): True}
        elif len(p) == 5:
            value = {f"{p[2].lower()}_{p[3].lower()}": int(p_list[-1])}
        if value:
            p[0].update(value)

    def p_seq_name(self, p: List) -> None:
        """seq_name : create_seq id DOT id
        | create_seq id
        """
        # get schema & table name
        p_list = list(p)
        schema = None
        if len(p) > 4:
            if "." in p:
                schema = p_list[-3]
                seq_name = p_list[-1]
        else:
            seq_name = p_list[-1]
        p[0] = {"schema": schema, "sequence_name": seq_name}

    def p_create_seq(self, p: List) -> None:
        """create_seq : CREATE SEQUENCE IF NOT EXISTS
        | CREATE SEQUENCE

        """
        # get schema & table name

        self.add_if_not_exists(p[0], list(p))

    def p_tid(self, p: List) -> None:
        """tid : LT id
        | LT
        | tid LT
        | tid id
        | tid COMMAT
        | tid RT
        """
        if not isinstance(p[1], list):
            p[0] = [p[1]]
        else:
            p[0] = p[1]

        for i in list(p)[2:]:
            if not i == "[]" and not i == ",":
                p[0][0] += f" {i}"
            else:
                p[0][0] += f"{i}"

    @staticmethod
    def get_complex_type(p, p_list):
        if len(p_list) == 4:
            p[0]["type"] = f"{p[2]} {p[3][0]}"
        elif p[0]["type"]:
            if len(p[0]["type"]) == 1 and isinstance(p[0]["type"], list):
                p[0]["type"] = p[0]["type"][0]
            p[0]["type"] = f'{p[0]["type"]} {p_list[-1][0]}'
        else:
            p[0]["type"] = p_list[-1][0]
        return p[0]

    def extract_references(self, table_data: Dict):
        ref = {
            "table": table_data["table_name"],
            "columns": [None],
            "schema": table_data["schema"],
            "on_delete": None,
            "on_update": None,
            "deferrable_initially": None,
        }

        if table_data.get("project"):
            ref["project"] = table_data["project"]

        return ref

    def p_dot_id_or_id(self, p: List) -> None:
        """dot_id_or_id : id
        | dot_id"""
        p[0] = p[1]

    def p_dot_id(self, p: List) -> None:
        """dot_id : id DOT id
        | dot_id DOT id"""
        p[0] = f"{p[1]}.{p[3]}"

    def p_null(self, p: List) -> None:
        """null : NULL
        | NOT NULL
        """
        nullable = True
        if "NULL" in p or "null" in p:
            if "NOT" in p or "not" in p:
                nullable = False
        p[0] = {"nullable": nullable}

    def p_f_call(self, p: List) -> None:
        """f_call : dot_id_or_id LP RP
        | id LP id RP
        | id LP RP
        | id LP f_call RP
        | id LP multi_id RP
        | id LP pid RP
        | id LP id AS id RP
        | dot_id_or_id LP id RP
        | dot_id_or_id LP f_call RP
        | dot_id_or_id LP multi_id RP
        | dot_id_or_id LP pid RP
        | dot_id_or_id LP id AS id RP
        """
        p_list = list(p)
        if isinstance(p[1], list):
            p[0] = p[1]
            p[0].append(p_list[-1])
        elif p_list[1].upper() == "CAST":
            p_list = remove_par(p_list)
            p[0] = {"cast": {"value": p_list[2], "as": p_list[4]}}
        else:
            value = ""
            for elem in p_list[1:]:
                if isinstance(elem, list):
                    elem = ",".join(elem)
                value += elem
            p[0] = value

    def p_multi_id(self, p: List) -> None:
        """multi_id : id
        | multi_id id
        | f_call
        | multi_id f_call
        """
        p_list = list(p)
        if isinstance(p[1], list):
            p[0] = p[1]
            p[0].append(p_list[-1])
        elif isinstance(p_list[1], dict):
            p[0] = p[1]
        else:
            value = " ".join(p_list[1:])
            p[0] = value

    def p_funct_args(self, p: List) -> None:
        """funct_args : LP multi_id RP"""
        p[0] = {"args": f"({p[2]})"}

    def p_funct(self, p: List) -> None:
        """funct : id LP multi_id RP"""
        p[0] = {"func_name": p[1], "args": f"({p[3]})"}

    def p_multiple_funct(self, p: List) -> None:
        """multiple_funct : funct
        | multiple_funct COMMA funct
        | multiple_funct COMMA
        """
        if not isinstance(p[1], list):
            p[0] = [p[1]]
        else:
            p[0] = p[1]
            p[0].append(p[-1])

    def p_funct_expr(self, p: List) -> None:
        """funct_expr : LP multi_id RP
        | multi_id
        """
        if len(p) > 2:
            p[0] = p[2]
        else:
            p[0] = p[1]

    def p_default(self, p: List) -> None:
        """default : DEFAULT id
        | DEFAULT STRING
        | DEFAULT NULL
        | default FOR dot_id_or_id
        | DEFAULT f_call
        | DEFAULT LP pid RP
        | DEFAULT LP funct_expr pid RP
        | default id
        | DEFAULT ID EQ id_or_string
        | DEFAULT funct_expr
        """
        p_list = remove_par(list(p))

        default = self.pre_process_default(p_list)
        if "DEFAULT" in p_list:
            index_default = p_list.index("DEFAULT")
            p_list = p_list[index_default:]
        if isinstance(p_list[-1], list):
            p_list[-1] = " ".join(p_list[-1])
            default = " ".join(p_list[1:])
        if default.isnumeric():
            default = int(default)
        if isinstance(p[1], dict):
            p[0] = self.process_dict_default_value(p_list, default)
        else:
            p[0] = {"default": default}

    @staticmethod
    def pre_process_default(p_list: List) -> Any:
        if "FOR" in p_list or "for" in p_list:
            return "FOR"
        if len(p_list) == 5:
            if isinstance(p_list[3], list):
                default = p_list[3][0]
            else:
                default = f"{''.join(p_list[2:5])}"
        elif "DEFAULT" in p_list and len(p_list) == 4:
            default = f"{p_list[2]} {p_list[3]}"
        else:
            default = p_list[-1]
        return default

    @staticmethod
    def process_dict_default_value(p_list: List, default: Any) -> Dict:
        data = p_list[1]
        if "FOR" in default:
            data["default"] = {"next_value_for": p_list[-1]}
        else:
            for i in p_list[2:]:
                if isinstance(p_list[2], str):
                    p_list[2] = p_list[2].replace("\\'", "'")
                    if i == ")" or i == "(":
                        data["default"] = str(data["default"]) + f"{i}"
                    else:
                        data["default"] = str(data["default"]) + f" {i}"
                    data["default"] = data["default"].replace("))", ")")
        return data

    def p_enforced(self, p: List) -> None:
        """enforced : ENFORCED
        | NOT ENFORCED
        """
        p_list = list(p)
        p[0] = {"enforced": len(p_list) == 1}

    def p_collate(self, p: List) -> None:
        """collate : COLLATE id
        | COLLATE STRING
        """
        p_list = list(p)
        p[0] = {"collate": p_list[-1]}

    def p_constraint(self, p: List) -> None:
        """
        constraint : CONSTRAINT id
        """

        p_list = list(p)

        p[0] = {"constraint": {"name": p_list[-1]}}

    def p_generated(self, p: List) -> None:
        """
        generated : gen_always funct_expr
        | gen_always funct_expr id
        | gen_always LP multi_id RP
        | gen_always f_call
        """
        p_list = list(p)
        stored = False
        if len(p) > 3 and p_list[-1].lower() == "stored":
            stored = True
        _as = p[2]

        p[0] = {"generated": {"always": True, "as": _as, "stored": stored}}

    def p_gen_always(self, p: List) -> None:
        """
        gen_always : GENERATED id AS
        """
        p[0] = {"generated": {"always": True}}

    def p_in_statement(self, p: List) -> None:
        """in_statement : ID IN LP pid RP"""
        p_list = list(p)
        p[0] = {}
        p[0]["in_statement"] = {"name": p[1], "in": p_list[-2]}

    def p_multi_id_statement(self, p: List) -> None:
        """multi_id_statement : id_or_string id_or_string
        | multi_id_statement id_or_string
        | multi_id_statement EQ id_or_string
        | multi_id_statement in_statement
        """
        p_list = list(p)
        p[0] = " ".join(p_list[1:])

    def p_check_st(self, p: List) -> None:
        """check_st : CHECK LP multi_id_statement RP
        | CHECK LP f_call id id RP
        | CHECK LP f_call id RP
        | CHECK LP f_call RP
        | CHECK LP id_equals
        | CHECK LP in_statement RP
        | check_st id
        | check_st STRING
        | check_st id STRING
        | check_st LP id RP
        | check_st STRING RP
        | check_st funct_args
        | CHECK LP id DOT id RP
        | CHECK LP id RP
        | CHECK LP pid RP
        | check_st id RP
        | check_st id_equals RP
        """
        p_list = remove_par(list(p))
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {"check": []}

        i = 0
        items = p_list[2:]
        items_num = len(items)

        while i < items_num:
            item = items[i]
            # handle <schema>.<function>
            if i + 1 < items_num and items[i + 1] == ".":
                p[0]["check"].append(f"{''.join(items[i:i + 3])}")
                i += 3
                continue
            if isinstance(p_list[-1], dict) and p_list[-1].get("args"):
                p[0]["check"][-1] += p_list[-1]["args"]
            elif isinstance(item, list):
                p[0]["check"].append(f"({','.join(item)})")
            else:
                p[0]["check"].append(item)
            i += 1

    def p_using_tablespace(self, p: List) -> None:
        """using_tablespace : USING INDEX tablespace"""
        p_list = list(p)
        p[0] = {"using": {"tablespace": p_list[-1], "index": True}}

    def p_pid(self, p: List) -> None:
        """pid :  id
        | STRING
        | pid id
        | pid STRING
        | STRING LP RP
        | id LP RP
        | pid COMMA id
        | pid COMMA STRING
        """
        p_list = list(p)

        if len(p_list) == 4 and isinstance(p[1], str):
            p[0] = ["".join(p[1:])]
        elif not isinstance(p_list[1], list):
            p[0] = [p_list[1]]
        else:
            p[0] = p_list[1]
            p[0].append(p_list[-1])

    def p_index_pid(self, p: List) -> None:
        """index_pid :  id
        | index_pid id
        | index_pid COMMA index_pid
        """
        p_list = list(p)
        if len(p_list) == 2:
            detailed_column = {"name": p_list[1], "order": "ASC", "nulls": "LAST"}
            column = p_list[1]
            p[0] = {"detailed_columns": [detailed_column], "columns": [column]}
        else:
            p[0] = p[1]
            if len(p) == 3:
                if p_list[-1] in ["DESC", "ASC"]:
                    p[0]["detailed_columns"][0]["order"] = p_list[-1]
                else:
                    p[0]["detailed_columns"][0]["nulls"] = p_list[-1]

                column = p_list[2]
            elif isinstance(p_list[-1], dict):
                for i in p_list[-1]["columns"]:
                    p[0]["columns"].append(i)
                for i in p_list[-1]["detailed_columns"]:
                    p[0]["detailed_columns"].append(i)

    def p_foreign(self, p):
        # todo: need to redone id lists
        """foreign : FOREIGN KEY LP pid RP
        | FOREIGN KEY"""
        p_list = remove_par(list(p))
        if len(p_list) == 4:
            columns = p_list[-1]
            p[0] = columns

    def p_ref(self, p: List) -> None:
        """ref : REFERENCES t_name
        | ref LP pid RP
        | ref ON DELETE id
        | ref ON UPDATE id
        | ref DEFERRABLE INITIALLY id
        | ref NOT DEFERRABLE
        """
        p_list = remove_par(list(p))
        if isinstance(p[1], dict):
            p[0] = p[1]
            if "ON" not in p_list and "DEFERRABLE" not in p_list:
                p[0]["references"]["columns"] = p_list[-1]
            else:
                p[0]["references"]["columns"] = p[0]["references"].get(
                    "columns", [None]
                )
        else:
            data = {"references": self.extract_references(p_list[-1])}
            p[0] = data
        p[0] = self.process_references_with_properties(p[0], p_list)

    @staticmethod
    def process_references_with_properties(data: Dict, p_list: List) -> Dict:
        if "ON" in p_list:
            if "DELETE" in p_list:
                data["references"]["on_delete"] = p_list[-1]
            elif "UPDATE" in p_list:
                data["references"]["on_update"] = p_list[-1]
        elif "DEFERRABLE" in p_list:
            if "NOT" not in p_list:
                data["references"]["deferrable_initially"] = p_list[-1]
            else:
                data["references"]["deferrable_initially"] = "NOT"
        return data

    def p_expression_primary_key(self, p):
        "expr : pkey"
        p[0] = p[1]

    def p_uniq(self, p: List) -> None:
        """uniq : UNIQUE LP pid RP
        | UNIQUE KEY id LP pid RP
        """
        p_list = remove_par(list(p))
        key_name = None
        if isinstance(p_list[1], str) and p_list[1].upper() == "UNIQUE":
            del p_list[1]
        if isinstance(p_list[1], str) and p_list[1].upper() == "KEY":
            del p_list[1]
        if len(p_list) > 2:
            # We have name and columns
            key_name = p_list[1]

        p[0] = {"unique_statement": {"columns": p_list[-1]}}
        if key_name is not None:
            p[0]["unique_statement"]["name"] = key_name

    def p_statem_by_id(self, p: List) -> None:
        """statem_by_id : id LP pid RP
        | id KEY LP pid RP
        """
        p_list = remove_par(list(p))
        if p[1].upper() == "UNIQUE":
            p[0] = {"unique_statement": p_list[-1]}
        elif p[1].upper() == "CHECK":
            p[0] = {"check": p_list[-1]}
        elif p[1].upper() == "PRIMARY":
            p[0] = {"primary_key": p_list[-1]}

    def p_pkey(self, p: List) -> None:
        """pkey : pkey_statement LP pid RP
        | pkey_statement ID LP pid RP
        """
        p_list = remove_par(list(p))
        columns = []

        p[0] = {}

        if isinstance(p_list[2], str) and "CLUSTERED" == p_list[2]:
            order = None
            column = None
            for item in p_list[-1]:
                if item not in ["ASC", "DESC"]:
                    column = item
                else:
                    order = item
                if column and order:
                    columns.append({"column": column, "order": order})
                    column = None
                    order = None
            p[0]["clustered_primary_key"] = columns

        p[0] = self.process_order_in_pk(p[0], p_list)

    @staticmethod
    def process_order_in_pk(data: Dict, p_list: List) -> Dict:
        columns = []
        for item in p_list[-1]:
            if item not in ["ASC", "DESC"]:
                columns.append(item)
        data["primary_key"] = columns
        return data

    def p_pkey_statement(self, p: List) -> None:
        """pkey_statement : PRIMARY KEY"""
        p[0] = {"primary_key": None}

    def p_comment(self, p: List) -> None:
        """comment : COMMENT STRING"""
        p_list = remove_par(list(p))
        p[0] = {"comment": check_spec(p_list[-1])}

    def p_tablespace(self, p: List) -> None:
        """tablespace : TABLESPACE id
        | TABLESPACE id properties
        """
        # Initial 5m Next 5m Maxextents Unlimited
        p[0] = self.get_tablespace_data(list(p))

    def p_expr_tablespace(self, p: List) -> None:
        """expr : expr tablespace"""
        p_list = list(p)
        p[0] = p[1]
        p[0]["tablespace"] = p_list[-1]

    def p_by_smthg(self, p):
        """by_smthg : BY id
        | BY ROW
        | BY LP pid RP
        """
        p_list = remove_par(list(p))
        p[0] = {"by": p_list[-1]}
