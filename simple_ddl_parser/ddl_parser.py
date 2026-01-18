from typing import Dict, List, Optional

from ply.lex import LexToken

from simple_ddl_parser import tokens as tok
from simple_ddl_parser.dialects import (
    HQL,
    MSSQL,
    PSQL,
    Athena,
    BaseSQL,
    BigQuery,
    IBMDb2,
    MySQL,
    Oracle,
    Redshift,
    Snowflake,
    SparkSQL,
)
from simple_ddl_parser.exception import SimpleDDLParserException
from simple_ddl_parser.parser import Parser


# "DDLParserError" is an alias for backward compatibility
class DDLParserError(SimpleDDLParserException):
    pass


class Dialects(
    SparkSQL,
    Snowflake,
    BaseSQL,
    HQL,
    MySQL,
    MSSQL,
    Oracle,
    Redshift,
    BigQuery,
    IBMDb2,
    PSQL,
    Athena,
):
    pass


class DDLParser(Parser, Dialects):
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
        elif not self.lexer.after_columns and self.lexer.columns_def:
            t.type = tok.columns_definition.get(t.value.upper(), t.type)
        return t

    def process_body_tokens(self, t: LexToken) -> LexToken:
        if (self.lexer.last_par == "RP" and not self.lexer.lp_open) or (
            self.lexer.after_columns and not self.lexer.columns_def
        ):
            t = self.after_columns_tokens(t)
        elif self.lexer.columns_def:
            t.type = tok.columns_definition.get(t.value.upper(), t.type)
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
        if t.value.startswith("ARRAY"):
            t.type = "ARRAY"
            return t
        elif self.lexer.is_like:
            t.type = tok.after_columns_tokens.get(t.value.upper(), t.type)
        elif not self.lexer.is_table:
            # if is_table mean wi already met INDEX or TABLE statement and
            # the definition already done and this is a string
            t.type = tok.definition_statements.get(
                t.value.upper(), t.type
            )  # Check for reserved word
        elif self.lexer.last_token != "COMMA":
            t.type = tok.common_statements.get(t.value.upper(), t.type)
        else:
            if not (self.lexer.columns_def and self.lexer.after_columns):
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

    def t_EQ(self, t: LexToken) -> LexToken:
        r"(=)+"
        t.type = "EQ"
        return self.set_last_token(t)

    def t_DOT(self, t: LexToken) -> LexToken:
        r"(\.)+"
        t.type = "DOT"
        return self.set_last_token(t)

    def t_STRING_BASE(self, t: LexToken) -> LexToken:
        r"((\')([a-zA-Z_,`0-9:><\=\-\+.\~\%$\!() {}\[\]\/\\\"\#\*&^|?;±§@~]*)(\')){1}"
        t.type = "STRING_BASE"
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
            "CONSTRAINT",
            "EXISTS",
        ]
        return (
            t.value not in skip_id_tokens
            and t.value.upper() not in ["IF"]
            and (
                self.lexer.last_token in exceptional_keys
                or (
                    self.lexer.last_token == "INDEX" and self.lexer.is_table is not True
                )
            )
            and not self.exceptional_cases(t.value.upper())
        )

    def exceptional_cases(self, value: str) -> bool:
        if value == "TABLESPACE" and self.lexer.last_token == "INDEX":
            return True
        return False

    def t_COLLATE(self, t: LexToken):
        r"(?i:COLLATE|COLLATE)\b"
        if not self.lexer.after_columns:
            t.type = "COLLATE"
        else:
            t.type = "ID"
        return self.set_last_token(t)

    def t_AUTOINCREMENT(self, t: LexToken):
        r"(?i:AUTO_INCREMENT|AUTOINCREMENT)\b"
        if not self.lexer.after_columns:
            t.type = "AUTOINCREMENT"
        else:
            t.type = "ID"
        return self.set_last_token(t)

    def t_ID(self, t: LexToken):
        r"([0-9]+[.][0-9]*([e][+-]?[0-9]+)?|[0-9]\.[0-9])\w|([a-zA-Z_,0-9:><\/\\\=\-\+\~\%$@#\|&?;*\()!{}\[\]\`\[\]]+)"
        if len(t.value) > 1 and t.value.endswith(","):
            t.value = t.value[:-1]
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

        if self.lexer.is_alter:
            _type = tok.alter_tokens.get(t.value)
            if _type:
                t.type = _type

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

    def set_parenthesis_tokens(self, t: LexToken):
        if t.type in ["RP", "LP"]:
            if t.type == "RP" and self.lexer.lp_open:
                self.lexer.lp_open -= 1
                if not self.lexer.lp_open:
                    self.lexer.after_columns = True
            self.lexer.last_par = t.type

    def set_lexx_tags(self, t: LexToken):
        self.set_parenthesis_tokens(t)

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
        delimiters_to_start = ["`", '"', "["]
        delimiters_to_end = ["`", '"', "]"]
        p[0] = p[1]

        if self.normalize_names and len(p[0]) > 2:
            for num, symbol in enumerate(delimiters_to_start):
                if p[0].startswith(symbol) and p[0].endswith(delimiters_to_end[num]):
                    p[0] = p[0][1:-1]

    def p_id_or_string(self, p):
        """id_or_string : id
        | STRING"""
        p[0] = p[1]

    def p_string(self, p):
        """STRING : STRING_BASE
        | STRING STRING_BASE
        """
        p[0] = "".join(list(p[1:]))

    def t_error(self, t: LexToken):
        raise DDLParserError("Unknown symbol %r" % (t.value[0],))

    def p_error(self, p):
        if not self.silent:
            raise DDLParserError(f"Unknown statement at {p}")


def parse_from_file(
    file_path: str,
    encoding: Optional[str] = "utf-8",
    parser_settings: Optional[dict] = None,
    **kwargs,
) -> List[Dict]:
    """get useful data from ddl"""
    with open(file_path, "r", encoding=encoding) as df:
        return DDLParser(df.read(), **(parser_settings or {})).run(
            file_path=file_path, **kwargs
        )
