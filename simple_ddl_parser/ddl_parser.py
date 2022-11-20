from typing import Dict, List, Optional

from ply.lex import LexToken

from simple_ddl_parser import tokens as tok
from simple_ddl_parser.dialects.bigquery import BigQuery
from simple_ddl_parser.dialects.hql import HQL
from simple_ddl_parser.dialects.mssql import MSSQL
from simple_ddl_parser.dialects.mysql import MySQL
from simple_ddl_parser.dialects.oracle import Oracle
from simple_ddl_parser.dialects.redshift import Redshift
from simple_ddl_parser.dialects.snowflake import Snowflake
from simple_ddl_parser.dialects.spark_sql import SparkSQL
from simple_ddl_parser.dialects.sql import BaseSQL
from simple_ddl_parser.parser import Parser


class DDLParserError(Exception):
    pass


class DDLParser(
    Parser, SparkSQL, Snowflake, BaseSQL, HQL, MySQL, MSSQL, Oracle, Redshift, BigQuery
):

    tokens = tok.tokens
    t_ignore = "\t  \r"

    def get_tag_symbol_value_and_increment(self, t: LexToken) -> LexToken:
        # todo: need to find less hacky way to parse HQL structure types
        if "<" in t.value:
            t.type = "LT"
            self.lexer.lt_open += t.value.count("<")
        if ">" in t.value and not self.lexer.check:
            t.type = "RT"
            self.lexer.lt_open -= t.value.count(">")
        return t

    def after_columns_tokens(self, t: LexToken) -> LexToken:
        t.type = tok.after_columns_tokens.get(t.value.upper(), t.type)
        if t.type != "ID":
            self.lexer.after_columns = True
        elif self.lexer.columns_def:
            t.type = tok.columns_defenition.get(t.value.upper(), t.type)
        return t

    def process_body_tokens(self, t: LexToken) -> LexToken:
        if (
            self.lexer.last_par == "RP" and not self.lexer.lp_open
        ) or self.lexer.after_columns:
            t = self.after_columns_tokens(t)
        elif self.lexer.columns_def:
            t.type = tok.columns_defenition.get(t.value.upper(), t.type)
        elif self.lexer.sequence:
            t.type = tok.sequence_reserved.get(t.value.upper(), "ID")
        return t

    def parse_tags_symbols(self, t) -> Optional[LexToken]:
        """like symbols < >"""
        if not self.lexer.check:
            for key in tok.symbol_tokens_no_check:
                if key in t.value:
                    return self.get_tag_symbol_value_and_increment(t)

    def tokens_not_columns_names(self, t: LexToken) -> LexToken:

        t_tag = self.parse_tags_symbols(t)
        if t_tag:
            return t_tag

        if "ARRAY" in t.value:
            t.type = "ARRAY"
            return t
        elif self.lexer.is_like:
            t.type = tok.after_columns_tokens.get(t.value.upper(), t.type)
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

    def set_lexer_tags(self, t: LexToken) -> None:
        if t.type == "SEQUENCE":
            self.lexer.sequence = True
        elif t.type == "CHECK":
            self.lexer.check = True

    def t_DOT(self, t: LexToken) -> LexToken:
        r"\."
        t.type = "DOT"
        return self.set_last_token(t)

    def t_STRING(self, t: LexToken) -> LexToken:
        r"((\')([a-zA-Z_,`0-9:><\=\-\+.\~\%$\!() {}\[\]\/\\\"\#\*&^|?;±§@~]*)(\')){1}"
        t.type = "STRING"
        return self.set_last_token(t)

    def t_DQ_STRING(self, t: LexToken) -> LexToken:
        r"((\")([a-zA-Z_,`0-9:><\=\-\+.\~\%$\!() {}'\[\]\/\\\\#\*&^|?;±§@~]*)(\")){1}"
        t.type = "DQ_STRING"
        return self.set_last_token(t)

    def is_token_column_name(self, t: LexToken) -> bool:
        """many of reserved words can be used as column name,
        to decide is it a column name or not we need do some checks"""
        skip_id_tokens = ["(", ")", ","]
        return (
            t.value not in skip_id_tokens
            and self.lexer.is_table
            and self.lexer.lp_open
            and not self.lexer.is_like
            and (self.lexer.last_token == "COMMA" or self.lexer.last_token == "LP")
            and t.value.upper() not in tok.first_liners
        )

    def is_creation_name(self, t: LexToken) -> bool:
        """many of reserved words can be used as column name,
        to decide is it a column name or not we need do some checks"""
        skip_id_tokens = ["(", ")", ","]
        exceptional_keys = [
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
        return (
            t.value not in skip_id_tokens
            and t.value.upper() not in ["IF"]
            and self.lexer.last_token in exceptional_keys
            and not self.exceptional_cases(t.value.upper())
        )

    def exceptional_cases(self, value: str) -> bool:
        if value == "TABLESPACE" and self.lexer.last_token == "INDEX":
            return True
        return False

    def t_AUTOINCREMENT(self, t: LexToken):
        r"(AUTO_INCREMENT|AUTOINCREMENT)(?i)\b"
        t.type = "AUTOINCREMENT"
        return self.set_last_token(t)

    def t_ID(self, t: LexToken):
        r"([0-9]+[.][0-9]*([e][+-]?[0-9]+)?|[0-9]\.[0-9])\w|([a-zA-Z_,0-9:><\/\\\=\-\+\~\%$@#\|&?;*\()!{}\[\]\`\[\]]+)"
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

    def commat_type(self, t: LexToken):
        if t.type == "COMMA" and self.lexer.lt_open:
            t.type = "COMMAT"

    def capitalize_tokens(self, t: LexToken):
        if t.type != "ID" and t.type not in ["LT", "RT"]:
            t.value = t.value.upper()

    def set_parathesis_tokens(self, t: LexToken):
        if t.type in ["RP", "LP"]:
            if t.type == "RP" and self.lexer.lp_open:
                self.lexer.lp_open -= 1
            self.lexer.last_par = t.type

    def set_lexx_tags(self, t: LexToken):
        self.set_parathesis_tokens(t)

        if t.type == "ALTER":
            self.lexer.is_alter = True
        if t.type == "LIKE":
            self.lexer.is_like = True
        elif t.type in ["TYPE", "DOMAIN", "TABLESPACE"]:
            self.lexer.is_table = False
        elif t.type in ["TABLE", "INDEX"] and not self.lexer.is_alter:
            self.lexer.is_table = True

    def set_last_token(self, t: LexToken):
        self.lexer.last_token = t.type
        return t

    def p_id(self, p):
        """id : ID
        | DQ_STRING"""
        delimeters_to_start = ["`", '"', "["]
        delimeters_to_end = ["`", '"', "]"]
        p[0] = p[1]

        if self.normalize_names:
            for num, symbol in enumerate(delimeters_to_start):
                if p[0].startswith(symbol) and p[0].endswith(delimeters_to_end[num]):
                    p[0] = p[0][1:-1]

    def t_error(self, t: LexToken):
        raise DDLParserError("Unknown symbol %r" % (t.value[0],))

    def p_error(self, p):
        if not self.silent:
            raise DDLParserError(f"Unknown statement at {p}")


def parse_from_file(file_path: str, parser_settings: Optional[dict] = None, **kwargs) -> List[Dict]:
    """get useful data from ddl"""
    with open(file_path, "r") as df:
        return DDLParser(df.read(), **(parser_settings or {})).run(file_path=file_path, **kwargs)
