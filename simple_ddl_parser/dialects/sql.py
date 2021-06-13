import re
from copy import deepcopy
from typing import Dict

from simple_ddl_parser.utils import remove_par


class AfterColumns:
    def p_expression_partitioned_by(self, p):
        """expr : expr PARTITIONED BY LP pid RP"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["partitioned_by"] = p_list[-2]


class Table:
    def p_create_table(self, p):
        """create_table : CREATE TABLE IF NOT EXISTS
        | CREATE TABLE
        | CREATE ID TABLE IF NOT EXISTS
        | CREATE ID TABLE

        """
        # ID - for EXTERNAL
        # get schema & table name
        external = False
        if p[2].upper() == "EXTERNAL":
            external = True
        p[0] = {"external": external}


class Column:
    def set_base_column_propery(self, p: list) -> Dict:

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

    def p_column(self, p):
        """column : ID ID
        | ID ID DOT ID
        | ID tid
        | column comment
        | column LP ID RP
        | column ID
        | column LP ID COMMA ID RP
        | column ARRAY
        | ID ARRAY tid
        | column tid

        """
        p[0] = self.set_base_column_propery(p)
        p_list = remove_par(list(p))

        if "[]" == p_list[-1]:
            p[0]["type"] = p[0]["type"] + "[]"
        elif "ARRAY" in p_list[-1]:
            arr_split = p_list[-1].split("ARRAY")
            append = "[]" if not arr_split[-1] else arr_split[-1]
            p[0]["type"] = p[0]["type"] + append
        elif isinstance(p_list[-1], list):
            p[0] = self.get_complex_type(p, p_list)
        elif "comment" in p_list[-1]:
            p[0]["comment"] = p_list[-1]["comment"]
        else:
            match = re.match(r"[0-9]+", p_list[2])
            if bool(match) or p_list[2] == "max":
                if p_list[2].isnumeric():
                    size = int(p_list[2])
                else:
                    size = p_list[2]
                if len(p_list) == 3:
                    p[0]["size"] = size
                else:
                    p[0]["size"] = (int(p_list[2]), int(p_list[4]))
            elif isinstance(p_list[-1], str) and p_list[-1] not in p[0]["type"]:
                p[0]["type"] += f" {p_list[-1]}"

    def p_defcolumn(self, p):
        """defcolumn : column
        | defcolumn comment
        | defcolumn null
        | defcolumn PRIMARY KEY
        | defcolumn UNIQUE
        | defcolumn check_ex
        | defcolumn def
        | defcolumn ref
        | defcolumn foreign ref
        | defcolumn encrypt
        | defcolumn generated
        """
        pk = False
        nullable = True
        default = None
        unique = False
        check = None
        references = None
        p[0] = p[1]
        p_list = list(p)
        if ("KEY" in p or "key" in p) and ("PRIMARY" in p or "primary" in p):
            pk = True
            nullable = False
        elif "unique" in p or "UNIQUE" in p:
            unique = True
        elif isinstance(p_list[-1], dict) and "references" in p_list[-1]:
            p_list[-1]["references"]["column"] = p_list[-1]["references"]["columns"][0]
            del p_list[-1]["references"]["columns"]
            references = p_list[-1]["references"]
        for item in p[1:]:
            if isinstance(item, dict):
                p[0].update(item)

        p[0]["references"] = p[0].get("references", references)
        p[0]["unique"] = unique if unique is not False else p[0].get("unique", unique)
        p[0]["primary_key"] = pk if pk is not False else p[0].get("primary_key", pk)
        p[0]["nullable"] = (
            nullable if nullable is not True else p[0].get("nullable", nullable)
        )
        p[0]["default"] = p[0].get("default", default)
        p[0]["check"] = p[0].get("check", check)
        if p[0]["check"]:
            p[0]["check"] = " ".join(p[0]["check"])

    def p_check_ex(self, p):
        """check_ex :  check_st
        | constraint check_st
        """
        name = None
        if isinstance(p[1], dict):
            if "constraint" in p[1]:
                p[0] = {
                    "check": {
                        "constraint_name": p[1]["constraint"]["name"],
                        "statement": " ".join(p[2]["check"]),
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
    def p_expression_schema(self, p):
        """expr : create
        | create ID
        """
        p[0] = p[1]
        if len(p) > 2:
            p[0]["authorization"] = p[2]

    def p_create(self, p):
        """create : CREATE ID ID ID
        | CREATE ID ID
        | CREATE ID IF NOT EXISTS ID
        """
        p_list = list(p)
        if p_list[-2] == "AUTHORIZATION":
            p[0] = {f"{p[2].lower()}_name": p_list[-1], "authorization": p_list[-1]}
        elif p_list[-1] == "AUTHORIZATION":
            p[0] = {f"{p[2].lower()}_name": p_list[-2], "authorization": None}
        else:
            p[0] = {f"{p[2].lower()}_name": p_list[-1]}


class Drop:
    def p_expression_drop_table(self, p):
        """expr : DROP TABLE ID
        | DROP TABLE ID DOT ID
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
    def p_multiple_column_names(self, p):
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

    def p_expression_type_as(self, p):
        """expr : type_name ID LP pid RP
        | type_name ID LP multiple_column_names RP
        | type_name LP id_equals RP
        """
        p_list = list(p)
        p[0] = p[1]
        p[0]["base_type"] = p[2]
        p[0]["properties"] = {}
        base_type = p[0]["base_type"].upper()
        if base_type == "ENUM":
            p[0]["properties"]["values"] = p_list[4]
        elif p[0]["base_type"] == "OBJECT":
            if "type" in p_list[4][0]:
                p[0]["properties"]["attributes"] = p_list[4]
        else:
            if isinstance(p_list[-2], list):
                for item in p_list[-2]:
                    p[0]["properties"].update(item)

    def p_type_name(self, p):
        """type_name : type_create ID AS
        | type_create ID DOT ID AS
        | type_create ID DOT ID
        | type_create ID
        """
        p_list = list(p)
        p[0] = {}
        if "." not in p_list:
            p[0]["schema"] = None
            p[0]["type_name"] = p_list[2]
        else:
            p[0]["schema"] = p[2]
            p[0]["type_name"] = p_list[4]

    def p_type_create(self, p):
        """type_create : CREATE TYPE
        | CREATE OR REPLACE TYPE
        """
        p[0] = None


class Domain:
    def p_expression_domain_as(self, p):
        """expr : domain_name ID LP pid RP"""
        p_list = list(p)
        p[0] = p[1]
        p[0]["base_type"] = p[2]
        p[0]["properties"] = {}
        if p[0]["base_type"] == "ENUM":
            p[0]["properties"]["values"] = p_list[4]

    def p_domain_name(self, p):
        """domain_name : CREATE DOMAIN ID AS
        | CREATE DOMAIN ID DOT ID AS
        | CREATE DOMAIN ID DOT ID
        | CREATE DOMAIN ID
        """
        p_list = list(p)
        p[0] = {}
        if "." not in p_list:
            p[0]["schema"] = None
        else:
            p[0]["schema"] = p[3]
        p[0]["domain_name"] = p_list[-2]


class BaseSQL(Table, Drop, Domain, Column, AfterColumns, Type, Schema):
    def p_id_equals(self, p):
        """id_equals : ID ID ID
        | id_equals COMMA
        | id_equals COMMA ID ID ID
        """
        p_list = list(p)
        if "=" == p_list[-2]:
            property = {p_list[-3]: p_list[-1]}
            if not isinstance(p[1], list):
                p[0] = [property]
            else:
                p[0] = p[1]
                p[0].append(property)

    def p_expression_index(self, p):
        """expr : index_table_name LP index_pid RP"""
        p_list = remove_par(list(p))
        p[0] = p[1]

        for item in ["detailed_columns", "columns"]:
            if item not in p[0]:
                p[0][item] = p_list[-1][item]
            else:
                p[0][item].extend(p_list[-1][item])

    def p_index_table_name(self, p):
        """index_table_name : create_index ON ID
        | create_index ON ID DOT ID
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

    def p_create_index(self, p):
        """create_index : CREATE INDEX ID
        | CREATE UNIQUE INDEX ID
        | create_index ON ID
        | CREATE CLUSTERED INDEX ID
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

    def p_expression_table(self, p):
        """expr : table_name defcolumn
        | table_name LP defcolumn
        | expr COMMA defcolumn
        | expr COMMA
        | expr COMMA constraint
        | expr COMMA check_ex
        | expr COMMA foreign
        | expr COMMA pkey
        | expr COMMA uniq
        | expr COMMA statem_by_id
        | expr COMMA constraint uniq
        | expr COMMA constraint pkey
        | expr COMMA constraint foreign ref
        | expr COMMA foreign ref
        | expr RP
        """
        p[0] = p[1]
        p_list = list(p)
        if p_list[-1] != "," and p_list[-1] != ")":
            if "type" in p_list[-1] and "name" in p_list[-1]:
                p[0]["columns"].append(p_list[-1])
            elif "check" in p_list[-1]:
                if isinstance(p_list[-1]["check"], list):
                    check = " ".join(p_list[-1]["check"])
                    if isinstance(check, str):
                        check = {"constraint_name": None, "statement": check}
                else:
                    check = p_list[-1]["check"]
                    p[0] = self.set_constraint(
                        p[0], "checks", check, check["constraint_name"]
                    )
                p[0]["checks"].append(check)
            else:
                p[0].update(p_list[-1])

        if isinstance(p_list[-1], dict):
            if "constraint" in p_list[-2]:
                if p_list[-1].get("unique_statement"):
                    p[0] = self.set_constraint(
                        p[0],
                        "uniques",
                        {"columns": p_list[-1]["unique_statement"]},
                        p_list[-2]["constraint"]["name"],
                    )
                else:
                    p[0] = self.set_constraint(
                        p[0],
                        "primary_keys",
                        {"columns": p_list[-1]["primary_key"]},
                        p_list[-2]["constraint"]["name"],
                    )

            elif p_list[-1].get("references"):
                p[0] = self.add_ref_information_to_table(p, p_list)

    def add_ref_information_to_table(self, p, p_list):
        if len(p_list) > 4 and "constraint" in p_list[3]:
            p[0] = self.set_constraint(
                p[0],
                "references",
                p_list[-1]["references"],
                p_list[3]["constraint"]["name"],
            )
        elif isinstance(p_list[-2], list):
            if "ref_columns" not in p[0]:
                p[0]["ref_columns"] = []

            for num, column in enumerate(p_list[-2]):
                ref = deepcopy(p_list[-1]["references"])
                ref["column"] = ref["columns"][num]
                del ref["columns"]
                ref["name"] = column
                p[0]["ref_columns"].append(ref)
        return p[0]

    @staticmethod
    def set_constraint(target_dict, _type, constraint, constraint_name):
        if not target_dict.get("constraints"):
            target_dict["constraints"] = {}
        if not target_dict["constraints"].get(_type):
            target_dict["constraints"][_type] = []
        constraint.update({"constraint_name": constraint_name})
        target_dict["constraints"][_type].append(constraint)
        return target_dict

    def p_expression_like_table(self, p):
        """expr : table_name LIKE ID
        | table_name LIKE ID DOT ID
        """
        # get schema & table name
        p_list = list(p)
        if len(p) > 4:
            if "." in p:
                schema = p_list[-3]
                table_name = p_list[-1]
        else:
            table_name = p_list[-1]
            schema = None
        p[0] = p[1]
        p[0].update({"like": {"schema": schema, "table_name": table_name}})

    def p_table_name(self, p):
        """table_name : create_table ID DOT ID
        | create_table ID
        | table_name LIKE ID
        | table_name DOT ID
        """
        # get schema & table name
        p_list = list(p)
        p[0] = p[1]
        if len(p) > 4:
            if "." in p:
                schema = p_list[-3]
                table_name = p_list[-1]
        else:
            table_name = p_list[-1]
            schema = None

        p[0].update(
            {"schema": schema, "table_name": table_name, "columns": [], "checks": []}
        )

    def p_expression_seq(self, p):
        """expr : seq_name
        | expr INCREMENT ID
        | expr START ID
        | expr MINVALUE ID
        | expr MAXVALUE ID
        | expr CACHE ID
        """
        # get schema & table name
        p_list = list(p)
        p[0] = p[1]
        if len(p) > 2:
            p[0].update({p[2].lower(): int(p_list[-1])})

    def p_seq_name(self, p):
        """seq_name : create_seq ID DOT ID
        | create_seq ID
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

    def p_create_seq(self, p):
        """create_seq : CREATE SEQUENCE IF NOT EXISTS
        | CREATE SEQUENCE

        """
        # get schema & table name
        pass

    def p_tid(self, p):
        """tid : LT ID
        | tid ID
        | tid COMMAT
        | tid RT
        """
        if not isinstance(p[1], list):
            p[0] = [p[1]]
        else:
            p[0] = p[1]

        for i in list(p)[2:]:
            p[0][0] += i

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

    def extract_references(self, p_list):
        ref_index = p_list.index("REFERENCES")
        ref = {
            "table": None,
            "columns": [None],
            "schema": None,
            "on_delete": None,
            "on_update": None,
            "deferrable_initially": None,
        }
        if "." not in p_list[ref_index:]:
            ref.update({"table": p_list[ref_index + 1]})
            if not len(p_list) == 3:
                ref.update({"columns": p_list[-1]})
        else:
            ref.update(
                {
                    "schema": p_list[ref_index + 1],
                    "columns": p_list[-1],
                    "table": p_list[ref_index + 3],
                }
            )

        return ref

    def p_null(self, p):
        """null : NULL
        | NOT NULL
        """
        nullable = True
        if "NULL" in p or "null" in p:
            if "NOT" in p or "not" in p:
                nullable = False
        p[0] = {"nullable": nullable}

    def p_f_call(self, p):
        """f_call : ID LP RP
        | ID LP f_call RP
        | ID LP multi_id RP
        | ID LP pid RP
        """
        p_list = list(p)
        if isinstance(p[1], list):
            p[0] = p[1]
            p[0].append(p_list[-1])
        else:
            value = ""
            for elem in p_list[1:]:
                if isinstance(elem, list):
                    elem = ",".join(elem)
                value += elem
            p[0] = value

    def p_multi_id(self, p):
        """multi_id : ID
        | multi_id ID
        | f_call
        | multi_id f_call
        """
        p_list = list(p)
        if isinstance(p[1], list):
            p[0] = p[1]
            p[0].append(p_list[-1])
        else:
            value = " ".join(p_list[1:])
            p[0] = value

    def p_funct_expr(self, p):
        """funct_expr : LP multi_id RP
        | multi_id
        """
        if len(p) > 2:
            p[0] = p[2]
        else:
            p[0] = p[1]

    def p_def(self, p):
        """def : DEFAULT ID
        | DEFAULT STRING
        | DEFAULT NULL
        | DEFAULT funct_expr
        | DEFAULT LP pid RP
        | def ID
        | def LP RP
        """
        p_list = list(p)
        if len(p_list) == 5 and isinstance(p[3], list):
            default = p[3][0]
        else:
            default = p[2]

        if default.isnumeric():
            default = int(default)
        if isinstance(p[1], dict):
            p[0] = p[1]
            for i in p[2:]:
                if isinstance(p[2], str):
                    p[2] = p[2].replace("\\'", "'")
                    if i == ")" or i == "(":
                        p[0]["default"] = str(p[0]["default"]) + f"{i}"
                    else:
                        p[0]["default"] = str(p[0]["default"]) + f" {i}"
                    p[0]["default"] = p[0]["default"].replace("))", ")")
        else:
            p[0] = {"default": default}

    def p_constraint(self, p):
        """
        constraint : CONSTRAINT ID
        """

        p_list = list(p)
        p[0] = {"constraint": {"name": p_list[-1]}}

    def p_generated(self, p):
        """
        generated : gen_always funct_expr
        | gen_always funct_expr ID
        | gen_always LP multi_id RP
        | gen_always f_call
        """
        p_list = list(p)
        stored = False
        if len(p) > 3 and p_list[-1].lower() == "stored":
            stored = True
        _as = p[2]
        p[0] = {"generated": {"always": True, "as": _as, "stored": stored}}

    def p_gen_always(self, p):
        """
        gen_always : GENERATED ID AS
        """
        p[0] = {"generated": {"always": True}}

    def p_check_st(self, p):
        """check_st : CHECK LP ID
        | check_st ID
        | check_st STRING
        | check_st ID RP
        | check_st STRING RP
        """
        p_list = remove_par(list(p))
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {"check": []}
        for item in p_list[2:]:
            p[0]["check"].append(item)

    def p_expression_alter(self, p):
        """expr : alter_foreign ref
        | alter_check
        | alter_unique
        | alter_default
        """
        p[0] = p[1]
        if len(p) == 3:
            p[0].update(p[2])

    def p_alter_unique(self, p):
        """alter_unique : alt_table UNIQUE LP pid RP
        | alt_table constraint UNIQUE LP pid RP
        """

        p_list = remove_par(list(p))
        p[0] = p[1]
        p[0]["unique"] = {"constraint_name": None, "columns": p_list[-1]}
        if "constraint" in p[2]:
            p[0]["unique"]["constraint_name"] = p[2]["constraint"]["name"]

    def p_alter_default(self, p):
        """alter_default : alt_table ID ID
        | alt_table constraint ID ID
        | alt_table ID STRING
        | alt_table constraint ID STRING
        | alter_default ID
        | alter_default FOR pid
        """

        p_list = remove_par(list(p))
        p[0] = p[1]

        if "FOR" in p_list:
            column = p_list[-1]
            value = None
        elif p[0].get("default") and "value" in p[0]["default"]:
            value = p[0]["default"]["value"] + " " + p_list[-1]
            column = None
        else:
            value = p_list[-1]
            column = None
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
        if "constraint" in p[2]:
            p[0]["default"]["constraint_name"] = p[2]["constraint"]["name"]

    def p_alter_check(self, p):
        """alter_check : alt_table check_st
        | alt_table constraint check_st
        """
        p_list = remove_par(list(p))
        p[0] = p[1]
        if isinstance(p[1], dict):
            p[0] = p[1]
        if not p[0].get("check"):
            p[0]["check"] = {"constraint_name": None, "statement": []}
        if isinstance(p[2], dict) and "constraint" in p[2]:
            p[0]["check"]["constraint_name"] = p[2]["constraint"]["name"]
        p[0]["check"]["statement"] = p_list[-1]["check"]

    def p_pid_with_type(self, p):
        """pid_with_type :  column
        | pid_with_type COMMA column
        """
        p_list = list(p)
        if not isinstance(p_list[1], list):
            p[0] = [p_list[1]]
        else:
            p[0] = p_list[1]
            p[0].append(p_list[-1])

    def p_pid(self, p):
        """pid :  ID
        | STRING
        | pid ID
        | pid STRING
        | STRING LP RP
        | ID LP RP
        | pid COMMA ID
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

    def p_index_pid(self, p):
        """index_pid :  ID
        | index_pid ID
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

    def p_alter_foreign(self, p):
        """alter_foreign : alt_table foreign
        | alt_table constraint foreign
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
            if isinstance(p_list[2], dict) and "constraint" in p_list[2]:
                column.update({"constraint_name": p_list[2]["constraint"]["name"]})

    def p_alt_table_name(self, p):
        """alt_table : ALTER TABLE ID ADD
        | ALTER TABLE ID DOT ID ADD
        """
        p_list = list(p)
        if "." in p:
            idx_dot = p_list.index(".")
            schema = p_list[idx_dot - 1]
            table_name = p_list[idx_dot + 1]
        else:
            schema = None
            table_name = p_list[3]
        p[0] = {"alter_table_name": table_name, "schema": schema}

    def p_foreign(self, p):
        # todo: need to redone id lists
        """foreign : FOREIGN KEY LP pid RP
        | FOREIGN KEY"""
        p_list = remove_par(list(p))
        if len(p_list) == 4:
            columns = p_list[-1]
            p[0] = columns

    def p_ref(self, p):
        """ref : REFERENCES ID DOT ID
        | REFERENCES ID
        | ref LP pid RP
        | ref ON DELETE ID
        | ref ON UPDATE ID
        | ref DEFERRABLE INITIALLY ID
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
            data = {"references": self.extract_references(p_list)}
            p[0] = data
        if "ON" in p_list:
            if "DELETE" in p_list:
                p[0]["references"]["on_delete"] = p_list[-1]
            elif "UPDATE" in p_list:
                p[0]["references"]["on_update"] = p_list[-1]
        elif "DEFERRABLE" in p_list:
            if "NOT" not in p_list:
                p[0]["references"]["deferrable_initially"] = p_list[-1]
            else:
                p[0]["references"]["deferrable_initially"] = "NOT"

    def p_expression_primary_key(self, p):
        "expr : pkey"
        p[0] = p[1]

    def p_uniq(self, p):
        """uniq : UNIQUE LP pid RP"""
        p_list = remove_par(list(p))
        p[0] = {"unique_statement": p_list[-1]}

    def p_statem_by_id(self, p):
        """statem_by_id : ID LP pid RP
        | ID KEY LP pid RP
        """
        p_list = remove_par(list(p))
        if p[1].upper() == "UNIQUE":
            p[0] = {"unique_statement": p_list[-1]}
        elif p[1].upper() == "CHECK":
            p[0] = {"check": p_list[-1]}
        elif p[1].upper() == "PRIMARY":
            p[0] = {"primary_key": p_list[-1]}

    def p_pkey(self, p):
        """pkey : PRIMARY KEY LP pid RP"""
        p_list = remove_par(list(p))
        p[0] = {"primary_key": p_list[-1]}

    def p_comment(self, p):
        """comment : ID STRING"""
        p_list = remove_par(list(p))
        p[0] = {"comment": p_list[-1]}

    def p_tablespace(self, p):
        """tablespace : TABLESPACE ID"""
        # Initial 5m Next 5m Maxextents Unlimited
        p[0] = p[2]

    def p_expr_tablespace(self, p):
        """expr : expr tablespace"""
        p_list = list(p)
        p[0] = p[1]
        p[0]["tablespace"] = p_list[-1]
