from typing import Dict, List

from simple_ddl_parser import tokens as tok
from simple_ddl_parser.dialects.bigquery import BigQuery
from simple_ddl_parser.dialects.hql import HQL
from simple_ddl_parser.dialects.mssql import MSSQL
from simple_ddl_parser.dialects.mysql import MySQL
from simple_ddl_parser.dialects.oracle import Oracle
from simple_ddl_parser.dialects.redshift import Redshift
from simple_ddl_parser.dialects.snowflake import Snowflake
from simple_ddl_parser.dialects.sql import BaseSQL
from simple_ddl_parser.parser import Parser


class DDLParser(
    Parser, Snowflake, BaseSQL, HQL, MySQL, MSSQL, Oracle, Redshift, BigQuery
):

    tokens = tok.tokens
    t_ignore = "\t  \r"

    def get_tag_symbol_value_and_increment(self, t):
        # todo: need to find less hacky way to parse HQL structure types
        if "<" in t.value:
            t.type = "LT"
            self.lexer.lt_open += t.value.count("<")
        if ">" in t.value and not self.lexer.check:
            t.type = "RT"
            self.lexer.lt_open -= t.value.count(">")
        return t

    def after_columns_tokens(self, t):
        t.type = tok.after_columns_tokens.get(t.value.upper(), t.type)
        if t.type != "ID":
            self.lexer.after_columns = True
        elif self.lexer.columns_def:
            t.type = tok.columns_defenition.get(t.value.upper(), t.type)
        return t

    def process_body_tokens(self, t):
        if (
            self.lexer.last_par == "RP" and not self.lexer.lp_open
        ) or self.lexer.after_columns:
            t = self.after_columns_tokens(t)
        elif self.lexer.columns_def:
            t.type = tok.columns_defenition.get(t.value.upper(), t.type)
        elif self.lexer.sequence:
            t.type = tok.sequence_reserved.get(t.value.upper(), "ID")
        return t

    def tokens_not_columns_names(self, t):
        if not self.lexer.check:
            for key in tok.symbol_tokens_no_check:
                if key in t.value:
                    return self.get_tag_symbol_value_and_increment(t)
        if "ARRAY" in t.value:
            t.type = "ARRAY"
            return t
        elif not self.lexer.is_table:
            # if is_table mean wi already met INDEX or TABLE statement and
            # the definition already done and this is a string
            t.type = tok.defenition_statements.get(
                t.value.upper(), t.type
            )  # Check for reserved word
        elif self.lexer.last_token != "COMMA":
            t.type = tok.common_statements.get(t.value.upper(), t.type)
        else:
            t.type = tok.first_liners.get(t.value.upper(), t.type)

        # get tokens from other token dicts
        t = self.process_body_tokens(t)

        self.set_lexer_tags(t)

        return t

    def set_lexer_tags(self, t):
        if t.type == "SEQUENCE":
            self.lexer.sequence = True
        elif t.type == "CHECK":
            self.lexer.check = True

    def t_DOT(self, t):
        r"\."
        t.type = "DOT"
        return self.set_last_token(t)

    def t_STRING(self, t):
        r"((\')([a-zA-Z_,`0-9:><\=\-\+.\~\%$\!() {}\[\]\/\\\"\#\*&^|?;±§@~]*)(\')){1}"
        t.type = "STRING"
        return self.set_last_token(t)

    def t_DQ_STRING(self, t):
        r"((\")([a-zA-Z_,`0-9:><\=\-\+.\~\%$\!() {}'\[\]\/\\\\#\*&^|?;±§@~]*)(\")){1}"
        t.type = "DQ_STRING"
        return self.set_last_token(t)

    def is_token_column_name(self, t):
        """many of reserved words can be used as column name,
        to decide is it a column name or not we need do some checks"""
        skip_id_tokens = ["(", ")", ","]
        return (
            t.value not in skip_id_tokens
            and self.lexer.is_table
            and self.lexer.lp_open
            and (self.lexer.last_token == "COMMA" or self.lexer.last_token == "LP")
            and t.value.upper() not in tok.first_liners
        )

    def is_creation_name(self, t):
        """many of reserved words can be used as column name,
        to decide is it a column name or not we need do some checks"""
        skip_id_tokens = ["(", ")", ","]
        return (
            t.value not in skip_id_tokens
            and t.value.upper() not in ["IF"]
            and self.lexer.last_token
            in [
                "SCHEMA",
                "TABLE",
                "DATABASE",
                "TYPE",
                "DOMAIN",
                "TABLESPACE",
                "INDEX",
                "CONSTRAINT",
                "EXISTS",
            ]
        )

    def t_ID(self, t):
        r"([0-9]\.[0-9])\w|([a-zA-Z_,0-9:><\/\=\-\+\~\%$\*\()!{}\[\]\`\[\]]+)"
        t.type = tok.symbol_tokens.get(t.value, "ID")

        if t.type == "LP":
            self.lexer.lp_open += 1
            self.lexer.columns_def = True
            self.lexer.last_token = "LP"
            return t

        elif self.is_token_column_name(t) or self.lexer.last_token == "DOT":
            t.type = "ID"
        elif t.type != "DQ_STRING" and self.is_creation_name(t):
            t.type = "ID"
        else:
            t = self.tokens_not_columns_names(t)

        self.capitalize_tokens(t)
        self.commat_type(t)

        self.set_lexx_tags(t)

        return self.set_last_token(t)

    def commat_type(self, t):
        if t.type == "COMMA" and self.lexer.lt_open:
            t.type = "COMMAT"

    def capitalize_tokens(self, t):
        if t.type != "ID" and t.type not in ["LT", "RT"]:
            t.value = t.value.upper()

    def set_lexx_tags(self, t):
        if t.type in ["RP", "LP"]:
            if t.type == "RP" and self.lexer.lp_open:
                self.lexer.lp_open -= 1
            self.lexer.last_par = t.type
        elif t.type in ["TYPE", "DOMAIN", "TABLESPACE"]:
            self.lexer.is_table = False
        elif t.type in ["TABLE", "INDEX"]:
            self.lexer.is_table = True

    def set_last_token(self, t):
        self.lexer.last_token = t.type

        return t

    def p_id(self, p):
        """id : ID
        | DQ_STRING"""

        p[0] = p[1]

    def t_error(self, t):
        raise SyntaxError("Unknown symbol %r" % (t.value[0],))

    def p_error(self, p):
        pass


def parse_from_file(file_path: str, **kwargs) -> List[Dict]:
    """get useful data from ddl"""
    with open(file_path, "r") as df:
        return DDLParser(df.read()).run(file_path=file_path, **kwargs)
