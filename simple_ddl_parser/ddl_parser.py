import re
from typing import Dict, List
from simple_ddl_parser.parser import Parser


_ref = "REFERENCES"
_ref_lower = _ref.lower()
_def = "DEFAULT"
_def_lower = _def.lower()


class DDLParser(Parser):
    """
    lex and yacc parser for parse ddl into BQ schemas
    """

    reserved = {
        "IF": "IF",
        "then": "THEN",
        "else": "ELSE",
        "while": "WHILE",
        "USE": "USE",
        "CREATE": "CREATE",
        "TABLE": "TABLE",
        "NOT": "NOT",
        "EXISTS": "EXISTS",
        "NULL": "NULL",
        "NUM_VALUE_SDP": "NUM_VALUE_SDP",
        "PRIMARY": "PRIMARY",
        "KEY": "KEY",
        "DEFAULT": "DEFAULT",
        "REFERENCES": "REFERENCES",
        "ALTER": "ALTER",
        "ADD": "ADD",
        "FOREIGN": "FOREIGN",
    }

    tokens = tuple(["ID", "NEWLINE", "DOT"] + list(reserved.values()))

    t_ignore = "\t<>();\, '\"${}\r"
    t_DOT = r"."

    def t_NUM_VALUE_SDP(self, t):
        r"[0-9]+\D"
        t.type = "NUM_VALUE_SDP"
        t.value = re.sub(r"[\)\,]", "", t.value)
        return t

    def t_ID(self, t):
        r"[a-zA-Z_0-9:]+"
        t.type = self.reserved.get(t.value.upper(), "ID")  # Check for reserved word
        return t

    def t_newline(self, t):
        r"\n+"
        self.lexer.lineno += len(t.value)
        t.type = "NEWLINE"
        if self.lexer.paren_count == 0:
            return t

    def t_error(self, t):
        raise SyntaxError("Unknown symbol %r" % (t.value[0],))

    def p_error(self, p):
        pass

    def p_expression_table_name(self, p):
        """expr : create_table ID DOT ID
        | create_table ID
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

    def p_create_table(self, p):
        """create_table : CREATE TABLE IF NOT EXISTS
        | CREATE TABLE

        """
        # get schema & table name
        pass

    def p_column(self, p):
        """column : ID ID
        | ID ID NUM_VALUE_SDP
        """
        size = None
        type_str = p[2]
        if len(p) == 4:
            match = re.match(r"[0-9]+", p[3])
            if bool(match):
                size = int(p[3])
        p[0] = {"name": p[1], "type": type_str, "size": size}

    def extract_references(self, p_list):
        try:
            ref_index = p_list.index(_ref)
        except ValueError:
            ref_index = p_list.index(_ref_lower)
        if not "." in p_list[ref_index:]:
            references = {
                "table": p_list[ref_index + 1],
                "column": p_list[ref_index + 2],
                "schema": None,
            }
        else:
            references = {
                "schema": p_list[ref_index + 1],
                "column": p_list[ref_index + 4],
                "table": p_list[ref_index + 3],
            }
        return references

    def p_null(self, p):
        """null : NULL
        | NOT NULL
        """
        nullable = True
        if "NULL" in p or "null" in p:
            if "NOT" in p or "not" in p:
                nullable = False
        p[0] = {"nullable": nullable}

    def p_def(self, p):
        """def : DEFAULT ID
        | DEFAULT NUM_VALUE_SDP
        """
        p_list = list(p)
        try:
            ind_default = p_list.index(_def)
        except ValueError:
            ind_default = p_list.index(_def_lower)
        default = p[ind_default + 1]
        if default.isnumeric():
            default = int(default)
        p[0] = {"default": default}

    def p_defcolumn(self, p):
        """expr : column
        | expr null
        | expr PRIMARY KEY
        | expr def
        | expr ref
        """
        pk = False
        nullable = True
        default = None
        references = None
        p[0] = p[1]
        p_list = list(p)

        if ("KEY" in p or "key" in p) and ("PRIMARY" in p or "primary" in p):
            pk = True
            nullable = False
        if isinstance(p_list[-1], dict) and "references" in p_list[-1]:
            references = p_list[-1]["references"]
        for item in p[1:]:
            if isinstance(item, dict):
                p[0].update(item)
        p[0].update(
            {
                "primary_key": pk,
                "references": references,
            }
        )
        p[0]["nullable"] = p[0].get("nullable", nullable)
        p[0]["default"] = p[0].get("default", default)

    def p_expression_alter_table(self, p):
        # todo: need to redone id lists
        " expr : alter ref "
        p[0] = p[1]
        p[0].update(p[2])

    def p_alter(self, p):
        """alter : ALTER TABLE ID ADD foreign
        | ALTER TABLE ID DOT ID ADD foreign
        """
        p_list = list(p)
        if "." in p:
            idx_dot = p_list.index(".")
            schema = p_list[idx_dot - 1]
            table_name = p_list[idx_dot + 1]
        else:
            schema = None
            table_name = p_list[3]

        p[0] = {"alter_table_name": table_name, "schema": schema, "columns": p_list[-1]}

    def p_foreign(self, p):
        # todo: need to redone id lists
        """foreign : FOREIGN KEY ID
        | FOREIGN KEY ID ID
        | FOREIGN KEY ID ID ID
        | FOREIGN KEY ID ID ID ID
        | FOREIGN KEY ID ID ID ID ID
        """
        p_list = list(p)
        key_index = p_list.index("KEY")
        columns = p_list[key_index + 1 :]

        p[0] = columns

    def p_ref(self, p):
        """ref : REFERENCES ID ID
        | REFERENCES ID DOT ID ID
        | ref ID
        | ref ID ID
        | ref ID ID ID
        | ref ID ID ID ID
        """
        p_list = list(p)
        if isinstance(p[1], dict):
            p[0] = p[1]
            for column in p_list[2:]:
                p[0]["references"]["column"].append(column)
        else:
            data = {"references": self.extract_references(p_list)}
            p[0] = data

    def p_expression_primary_key(self, p):
        # todo: need to redone id lists
        """expr : PRIMARY KEY ID
        | PRIMARY KEY ID ID
        | PRIMARY KEY ID ID ID
        | PRIMARY KEY ID ID ID ID
        | PRIMARY KEY ID ID ID ID ID
        """
        p[0] = {"primary_key": [x for x in p[3:] if x != ","]}


def parse_from_file(file_path: str, **kwargs) -> List[Dict]:
    """ get useful data from ddl """
    with open(file_path, "r") as df:
        return DDLParser(df.read()).run(file_path=file_path, **kwargs)
