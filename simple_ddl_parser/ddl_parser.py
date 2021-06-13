from typing import Dict, List

from simple_ddl_parser import tokens as tok
from simple_ddl_parser.dialects.hql import HQL
from simple_ddl_parser.dialects.oracle import Oracle
from simple_ddl_parser.dialects.sql import BaseSQL
from simple_ddl_parser.parser import Parser


class DDLParser(Parser, BaseSQL, HQL, Oracle):
    """
    lex and yacc parser for parse ddl into BQ schemas
    """

    tokens = tok.tokens
    t_ignore = ";\t  \r"
    t_DOT = r"."

    def get_tag_symbol_value_and_increment(self, t):
        # todo: need to find less hacky way to parse HQL structure types
        if "<" == t.value:
            t.type = "LT"
            self.lexer.lt_open += 1
        elif ">" == t.value and not self.lexer.check:
            t.type = "RT"
            self.lexer.lt_open -= 1
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

    def t_STRING(self, t):
        r"((\')([a-zA-Z_,`0-9:><\=\-\+.\~\%$\!() {}\[\]\/\\\"]*)(\')){1}"
        t.type = "STRING"
        return t

    def t_ID(self, t):
        r"([0-9]\.[0-9])\w|([a-zA-Z_,0-9:><\/\=\-\+\~\%$\*'\()!{}\[\]\"]+)"
        t.type = tok.symbol_tokens.get(t.value, "ID")
        if t.type == "LP" and not self.lexer.after_columns:
            self.lexer.lp_open += 1
            self.lexer.columns_def = True
            return t
        elif not self.lexer.check and t.value in tok.symbol_tokens_no_check:
            return self.get_tag_symbol_value_and_increment(t)
        elif "ARRAY" in t.value:
            t.type = "ARRAY"
            return t
        elif not self.lexer.is_table:
            # if is_table mean wi already met INDEX or TABLE statement and
            # the defenition already done and this is a string
            t.type = tok.defenition_statements.get(
                t.value.upper(), t.type
            )  # Check for reserved word
        elif self.lexer.last_token != "COMMA":
            t.type = tok.common_statements.get(t.value.upper(), t.type)
        else:
            t.type = tok.first_liners.get(t.value.upper(), t.type)

        # get tokens from other token dicts
        t = self.process_body_tokens(t)

        if t.type == "SEQUENCE":
            self.lexer.sequence = True
        if t.type == "COMMA" and self.lexer.lt_open:
            t.type = "COMMAT"
        if t.type == "CHECK":
            self.lexer.check = True
        if t.type != "ID":
            t.value = t.value.upper()
        return self.set_last_token(t)

    def set_last_token(self, t):
        self.lexer.last_token = t.type
        if t.type in ["RP", "LP"]:
            if t.type == "RP" and self.lexer.lp_open:
                self.lexer.lp_open -= 1
            self.lexer.last_par = t.type
        elif t.type == "TYPE" or t.type == "DOMAIN":
            self.lexer.is_table = False
        elif t.type == "TABLE" or t.type == "INDEX":
            self.lexer.is_table = True
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


def parse_from_file(file_path: str, **kwargs) -> List[Dict]:
    """ get useful data from ddl """
    with open(file_path, "r") as df:
        return DDLParser(df.read()).run(file_path=file_path, **kwargs)
