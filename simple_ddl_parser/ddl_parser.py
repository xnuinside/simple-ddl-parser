import re
from typing import Dict, List
from simple_ddl_parser.parser import Parser


_ref = "REFERENCES"
_def = "DEFAULT"
_cons = "CONSTRAINT"


class DDLParser(Parser):
    """
    lex and yacc parser for parse ddl into BQ schemas
    """

    reserved = {
        "IF": "IF",
        "THEN": "THEN",
        "ON": "ON",
        "ELSE": "ELSE",
        "WHILE": "WHILE",
        "USE": "USE",
        "CREATE": "CREATE",
        "TABLE": "TABLE",
        "DROP": "DROP",
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
        "UNIQUE": "UNIQUE",
        "CHECK": "CHECK",
        "SEQUENCE": "SEQUENCE",
        "CONSTRAINT": "CONSTRAINT",
        "ARRAY": "ARRAY",
        "INDEX": "INDEX"
    }
    
    sequence = False
    sequence_reserved = {
        "INCREMENT": "INCREMENT",
        "START": "START",
        "MINVALUE": "MINVALUE",
        "MAXVALUE": "MAXVALUE",
        "CACHE": "CACHE",
    }
    tokens = tuple(
        ["ID", "NEWLINE", "DOT"]
        + list(reserved.values())
        + list(sequence_reserved.values())
    )

    t_ignore = '\t();,  "\r'
    t_DOT = r"."

    def t_NUM_VALUE_SDP(self, t):
        r"[0-9]+\D"
        t.type = "NUM_VALUE_SDP"
        t.value = re.sub(r"[\)\,;]", "", t.value)
        return t

    def t_ID(self, t):
        r"[a-zA-Z_0-9:><\=\-\+\~\%$\'!{}\[\]]+"

        if self.sequence:
            t.type = self.sequence_reserved.get(t.value.upper(), "ID")
        elif 'ARRAY' in t.value:
            t.type = 'ARRAY'
        else:
            t.type = self.reserved.get(t.value.upper(), "ID")  # Check for reserved word
        if t.type == "SEQUENCE":
            self.sequence = True
        if t.type != "ID":
            t.value = t.value.upper()
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
    
    def p_expression_index(self, p):
        """expr : index_table_name ID
                | expr ID
        """
        p_list = list(p)
        p[0] = p[1]
        if not 'columns' in p[0]:
            p[0]['columns'] = [p_list[-1]]
        else:
            p[0]['columns'].append(p_list[-1])
    
    def p_index_table_name(self, p):
        """index_table_name : create_index ON ID  
                | create_index ON ID DOT ID  
        """
        p[0] = p[1]
        p_list = list(p)
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
        """
        p_list = list(p)
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {"schema": None, "index_name": p_list[-1], "unique": 'UNIQUE' in p_list}
    
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

    def p_expression_seq(self, p):
        """expr : seq_name
        | expr INCREMENT NUM_VALUE_SDP
        | expr START NUM_VALUE_SDP
        | expr MINVALUE NUM_VALUE_SDP
        | expr MAXVALUE NUM_VALUE_SDP
        | expr CACHE NUM_VALUE_SDP
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

    def p_create_table(self, p):
        """create_table : CREATE TABLE IF NOT EXISTS
        | CREATE TABLE

        """
        # get schema & table name
        pass

    def p_column(self, p):
        """column : ID ID
                  | column NUM_VALUE_SDP 
                  | column ARRAY
                  | column ID 
                  
        """
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            size = None
            type_str = p[2]
            p[0] = {"name": p[1], "type": type_str, "size": size}
        p_list = list(p)
        
        if '[]' == p_list[-1]:
            p[0]['type'] = p[0]['type'] + '[]'
        elif 'ARRAY' in p_list[-1]:
            arr_split = p_list[-1].split('ARRAY')
            append = '[]' if not arr_split[-1] else arr_split[-1]
            p[0]['type'] = p[0]['type'] + append
        else:
            match = re.match(r"[0-9]+", p_list[2])
            if bool(match):
                size = int(p_list[2])
                p[0]['size'] = size
    def extract_references(self, p_list):
        ref_index = p_list.index(_ref)
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
        | def ID
        """
        p_list = list(p)
        default = p[2]
        if default.isnumeric():
            default = int(default)
        if isinstance(p[1], dict):
            p[0] = p[1]
            if isinstance(p[2], str):
                p[0]["default"] += f" {p[2]}"
                p[0]["default"] = p[0]["default"].replace('"', "").replace("'", "")
        else:
            p[0] = {"default": default}

    def p_defcolumn(self, p):
        """expr : column
        | expr null
        | expr PRIMARY KEY
        | expr UNIQUE
        | expr check_st
        | expr def
        | expr ref
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
        if "unique" in p or "UNIQUE" in p:
            unique = True
        if isinstance(p_list[-1], dict) and "references" in p_list[-1]:
            references = p_list[-1]["references"]
        for item in p[1:]:
            if isinstance(item, dict):
                p[0].update(item)
        p[0].update({"primary_key": pk, "references": references, "unique": unique})
        p[0]["nullable"] = p[0].get("nullable", nullable)
        p[0]["default"] = p[0].get("default", default)
        p[0]["check"] = p[0].get("check", check)
        if p[0]["check"]:
            p[0]["check"] = " ".join(p[0]["check"])

    def p_expr_check(self, p):
        """expr :  check_st
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

    def p_constraint(self, p):
        """
        constraint : CONSTRAINT ID
        """

        p_list = list(p)
        con_ind = p_list.index(_cons)
        name = p_list[con_ind + 1]
        p[0] = {"constraint": {"name": name}}

    def p_check_st(self, p):
        """check_st : CHECK ID
        | check_st ID
        | check_st ID ID
        """
        p_list = list(p)
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {"check": []}
        for item in p_list[2:]:
            p[0]["check"].append(item)

    def p_expression_alter(self, p):
        """expr : alter_foreign ref
        | alter_check
        """
        p[0] = p[1]
        if len(p) == 3:
            p[0].update(p[2])

    def p_alter_check(self, p):
        """alter_check : alt_table CHECK ID
        | alt_table constraint CHECK ID
        | alter_check ID
        """

        p_list = list(p)
        p[0] = p[1]
        if isinstance(p[1], dict):
            p[0] = p[1]
        if not p[0].get("check"):
            p[0]["check"] = {"constraint_name": None, "statement": []}
        if isinstance(p[2], dict) and "constraint" in p[2]:
            p[0]["check"]["constraint_name"] = p[2]["constraint"]["name"]
        p[0]["check"]["statement"].append(p_list[-1])

    def p_alter_foreign(self, p):
        """alter_foreign : alt_table foreign
        | alt_table constraint foreign
        """

        p_list = list(p)
        p[0] = p[1]
        if isinstance(p_list[-1], list):
            column = {"name": p_list[-1][0]}
        else:
            column = p_list[-1]
        if isinstance(p_list[2], dict) and "constraint" in p_list[2]:
            column.update({"constraint_name": p_list[2]["constraint"]["name"]})

        if not p[0].get("columns"):
            p[0]["columns"] = []
        p[0]["columns"].append(column)

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
        """foreign : FOREIGN KEY ID
        | foreign ID
        """
        p_list = list(p)
        key_index = p_list.index("KEY")
        columns = p_list[key_index + 1 :]

        p[0] = columns

    def p_ref(self, p):
        """ref : REFERENCES ID ID
        | REFERENCES ID DOT ID ID
        | ref ID
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
        "expr : pkey"
        p[0] = p[1]

    def p_expression_uniq(self, p):
        "expr : uniq"
        p[0] = p[1]

    def p_uniq(self, p):
        """uniq : UNIQUE ID
        | uniq ID
        """
        if isinstance(p[1], dict):
            p[0] = p[1]
            p[0]["unique"].append(p[2])
        else:
            p[0] = {"unique": [x for x in p[1:] if x != ","]}

    def p_pkey(self, p):
        """pkey : PRIMARY KEY ID
        | pkey ID
        """
        if isinstance(p[1], dict):
            p[0] = p[1]
            p[0]["primary_key"].append(p[2])
        else:
            p[0] = {"primary_key": [x for x in p[3:] if x != ","]}


def parse_from_file(file_path: str, **kwargs) -> List[Dict]:
    """ get useful data from ddl """
    with open(file_path, "r") as df:
        return DDLParser(df.read()).run(file_path=file_path, **kwargs)
