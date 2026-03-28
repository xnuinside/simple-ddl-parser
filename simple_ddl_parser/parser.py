import json
import logging
import os
import re
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Tuple, Union, cast

from ply import lex, yacc

from simple_ddl_parser.exception import SimpleDDLParserException
from simple_ddl_parser.output.core import Output, dump_data_to_file
from simple_ddl_parser.output.dialects import dialect_by_name
from simple_ddl_parser.utils import (
    find_first_unpair_closed_par,
    get_table_id,
    normalize_name,
)

# open comment
OP_COM = "/*"
# close comment
CL_COM = "*/"

IN_COM = "--"
MYSQL_COM = "#"

LF_IN_QUOTE = "pars_m_n"
UNNAMED_TABLE_KEY_RE = re.compile(r"^(\s*,?\s*)KEY(?=\s*\()", flags=re.IGNORECASE)

CREATE_TABLE_AS_SELECT_RE = re.compile(
    r"""
    ^\s*CREATE\s+TABLE\s+
    (?P<target>[^\s(]+)
    \s+AS\s+SELECT\s+
    (?P<select>.+?)
    \s+FROM\s+
    (?P<source>[^\s;]+)
    (?P<tail>\s+.*)?$
    """,
    flags=re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

CREATE_VIEW_RE = re.compile(
    r"""
    ^\s*CREATE\s+
    (?P<replace>OR\s+REPLACE\s+)?
    VIEW\s+
    (?P<target>[^\s(]+)
    \s+AS\s+
    (?P<definition>.+?)
    \s*$
    """,
    flags=re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

DROP_VIEW_RE = re.compile(
    r"""
    ^\s*DROP\s+VIEW\s+
    (?P<target>[^\s;]+)
    \s*$
    """,
    flags=re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

PARTITION_BY_RE = re.compile(r"\bPARTITION\s+BY\b", flags=re.IGNORECASE)


def set_logging_config(
    log_level: Union[str, int], log_file: Optional[str] = None
) -> None:
    if log_file:
        logging.basicConfig(
            level=log_level,
            filename=log_file,
            filemode="w",
            format="%(filename)10s:%(lineno)4d:%(message)s",
        )
    else:
        logging.basicConfig(
            level=log_level,
            format="%(filename)10s:%(lineno)4d:%(message)s",
        )


class Parser:
    """
    Base class for a lexer/parser that has the rules defined as methods

        It could not be loaded or called without Subclass,

        for example: DDLParser

        Subclass must include tokens for parser and rules

    This class contains logic for lines pre-processing before passing them to lexx&yacc parser:

        - clean up
        - catch comments
        - catch statements like 'SET' (they are not parsed by parser)
        - etc
    """

    INNER_TYPE_COMMENT_PREFIX = "SDP_INNER_TYPE_COMMENT_"

    def __init__(
        self,
        content: str,
        silent: bool = True,
        debug: bool = False,
        normalize_names: bool = False,
        log_file: Optional[str] = None,
        log_level: Union[str, int] = logging.INFO,
    ) -> None:
        """
        content: is a file content for processing
        silent: if true - will not raise errors, just return empty output
        debug: if True - parser will produce huge tokens tree & parser.out file, normally you don't want this enable
        normalize_names: if flag is True (default 'False') then all identifiers will be returned without
                        '[', '"' and other delimiters that used in different SQL dialects to separate custom names
                        from reserved words & statements.
                            For example, if flag set 'True' and you pass this input:

                            CREATE TABLE [dbo].[TO_Requests](
                                [Request_ID] [int] IDENTITY(1,1) NOT NULL,
                                [user_id] [int]

                        In output you will have names like 'dbo' and 'TO_Requests', not '[dbo]' and '[TO_Requests]'.
        log_file: path to file for logging
        log_level: set logging level for parser
        """
        self.tables = []
        self.silent = not debug if debug else silent
        self.inner_type_comments = {}
        self.has_generated_always_identity = bool(
            re.search(
                r"GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*\(",
                content,
                flags=re.IGNORECASE,
            )
        )
        self.has_generated_by_default_identity = bool(
            re.search(
                r"GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\s*\(",
                content,
                flags=re.IGNORECASE,
            )
        )
        content = self.normalize_inner_type_comments(content)
        content = self.normalize_generated_always_identity(content)
        self.data = content.encode("unicode_escape")
        self.paren_count = 0
        self.normalize_names = normalize_names
        set_logging_config(log_level, log_file)
        log = logging.getLogger()
        self.lexer = lex.lex(object=self, debug=False, debuglog=log)
        self.yacc = yacc.yacc(module=self, debug=False, debuglog=log)
        self.columns_closed = False
        self.statement = None
        self.block_comments = []
        self.comments = []
        self.statement_inline_comments = []

        # self.comma_only_str = re.compile(r"((\')|(' ))+(,)((\')|( '))+\B")
        self.equal_without_space = re.compile(r"(\b)=")
        self.in_comment = re.compile(r"((\")|(\'))+(.)*(--)+(.)*((\")|(\'))+")
        self.set_statement = re.compile(r"SET ")
        self.skip_regex = re.compile(r"^(GO|USE|INSERT|GRANT|DELETE|COMMIT)\b")
        self.skip_statement_regexes = [
            re.compile(pattern, flags=re.IGNORECASE | re.DOTALL)
            for pattern in [
                r"^\s*PRAGMA\b.*$",
                r"^\s*BEGIN(?:\s+TRANSACTION)?\b.*$",
                r"^\s*LOCK\s+TABLES\b.*$",
                r"^\s*UNLOCK\s+TABLES\b.*$",
                r"^\s*DROP\s+USER\b.*$",
                r"^\s*CREATE\s+USER\b.*$",
            ]
        ]

    def normalize_inner_type_comments(self, content: str) -> str:
        result = []
        angle_depth = 0
        idx = 0
        comment_idx = 0

        while idx < len(content):
            char = content[idx]
            if char == "<":
                angle_depth += 1
                result.append(char)
                idx += 1
                continue
            if char == ">":
                angle_depth = max(0, angle_depth - 1)
                result.append(char)
                idx += 1
                continue
            if (
                angle_depth
                and content[idx : idx + 7].upper() == "COMMENT"
                and (idx == 0 or not content[idx - 1].isalnum())
            ):
                end_idx = idx + 7
                while end_idx < len(content) and content[end_idx].isspace():
                    end_idx += 1
                if end_idx < len(content) and content[end_idx] == "'":
                    string_end = end_idx + 1
                    while string_end < len(content):
                        if content[string_end] == "'" and (
                            string_end + 1 >= len(content)
                            or content[string_end + 1] != "'"
                        ):
                            break
                        if (
                            content[string_end] == "'"
                            and string_end + 1 < len(content)
                            and content[string_end + 1] == "'"
                        ):
                            string_end += 2
                            continue
                        string_end += 1
                    if string_end < len(content):
                        placeholder = f"{self.INNER_TYPE_COMMENT_PREFIX}{comment_idx}"
                        self.inner_type_comments[placeholder] = content[
                            idx : string_end + 1
                        ]
                        result.append(placeholder)
                        comment_idx += 1
                        idx = string_end + 1
                        continue

            result.append(char)
            idx += 1

        return "".join(result)

    def restore_inner_type_comments(self, value: str) -> str:
        for placeholder, comment in self.inner_type_comments.items():
            value = value.replace(placeholder, comment)
        return value

    def strip_partition_definitions(self, statement: str) -> str:
        match = PARTITION_BY_RE.search(statement)
        if not match:
            return statement

        idx = match.end()
        paren_depth = 0
        while idx < len(statement):
            char = statement[idx]
            if char == "(":
                paren_depth += 1
            elif char == ")" and paren_depth > 0:
                paren_depth -= 1
                if paren_depth == 0:
                    probe = idx + 1
                    while probe < len(statement) and statement[probe].isspace():
                        probe += 1
                    if probe >= len(statement) or statement[probe] != "(":
                        break

                    token_probe = probe + 1
                    while (
                        token_probe < len(statement)
                        and statement[token_probe].isspace()
                    ):
                        token_probe += 1
                    if statement[token_probe : token_probe + 9].upper() != "PARTITION":
                        break

                    nested_depth = 1
                    token_probe += 1
                    while token_probe < len(statement) and nested_depth > 0:
                        if statement[token_probe] == "(":
                            nested_depth += 1
                        elif statement[token_probe] == ")":
                            nested_depth -= 1
                        token_probe += 1
                    if nested_depth == 0:
                        return statement[:probe] + statement[token_probe:]
                    break
            idx += 1

        return statement

    def catch_comment_or_process_line(self) -> str:
        if self.multi_line_comment:
            if self.multi_line_comment_collect_all:
                if OP_COM in self.line:
                    self.comments.append(self.line.split(OP_COM, 1)[1])
                else:
                    self.comments.append(self.line)
            elif OP_COM in self.line:
                self.comments.append(self.line.split(OP_COM, 1)[1])
            if CL_COM in self.line:
                self.multi_line_comment = False
                self.multi_line_comment_collect_all = False
            return ""
        if self.line.strip().startswith((MYSQL_COM, IN_COM)):
            return ""
        return self.process_inline_comments()

    def pre_process_line(self) -> None:
        # self.line = self.comma_only_str.sub("_ddl_parser_comma_only_str", self.line)
        self.line = self.equal_without_space.sub(" = ", self.line)
        self.line = UNNAMED_TABLE_KEY_RE.sub(r"\1INDEX", self.line, count=1)
        code_line = self.catch_comment_or_process_line()
        if self.line.startswith(OP_COM) and CL_COM not in self.line:
            self.multi_line_comment = True
            self.multi_line_comment_collect_all = bool(
                re.match(r"^\s*/\*+\s*$", self.line)
            )
        elif self.line.startswith(CL_COM):
            self.multi_line_comment = False
            self.multi_line_comment_collect_all = False
        self.line = code_line

    def process_in_comment(self, line: str) -> str:
        if self.in_comment.search(line):
            code_line = line
        else:
            splitted_line = line.split(IN_COM)
            code_line = splitted_line[0]
            comment = splitted_line[1]
            self.comments.append(comment)
            self.add_inline_column_comment(code_line, comment)
        return code_line

    def process_line_before_comment(self) -> str:
        """get useful codeline - remove comment"""
        if IN_COM in self.line:
            return self.process_in_comment(self.line)
        code_line = self.line
        while OP_COM in code_line and CL_COM in code_line[code_line.index(OP_COM) :]:
            start = code_line.index(OP_COM)
            end = code_line.index(CL_COM, start) + len(CL_COM)
            comment = code_line[start + len(OP_COM) : end]
            if code_line[end:].strip() == ";":
                comment += ";"
            self.comments.append(comment)
            code_line = f"{code_line[:start]}{code_line[end:]}"
        if CL_COM not in code_line and OP_COM not in code_line:
            return code_line
        return ""

    def process_inline_comments(self) -> str:
        """this method сatches comments like "create table ( # some comment" - inline this statement"""
        comment = None
        code_line = self.process_line_before_comment()
        if OP_COM in self.line and CL_COM not in self.line:
            splitted_line = self.line.split(OP_COM)
            code_line += splitted_line[0]
            comment = splitted_line[1]
            self.block_comments.append(OP_COM)
        if CL_COM in self.line and OP_COM not in self.line and self.block_comments:
            splitted_line = self.line.split(CL_COM)
            self.block_comments.pop(-1)
            code_line += splitted_line[1]
            if splitted_line[1].strip():
                comment = splitted_line[0]

        if comment:
            self.comments.append(comment)
        return code_line

    def add_inline_column_comment(self, code_line: str, comment: str) -> None:
        column_name = self.extract_column_name_from_line(code_line)
        if column_name:
            self.statement_inline_comments.append(
                {"name": column_name, "comment": comment}
            )

    @staticmethod
    def extract_column_name_from_line(line: str) -> Optional[str]:
        stripped = line.strip().lstrip(",").strip()
        if not stripped or stripped.startswith(")"):
            return None
        first_token = stripped.split(None, 1)[0].upper()
        if first_token in {
            "CONSTRAINT",
            "PRIMARY",
            "FOREIGN",
            "UNIQUE",
            "CHECK",
            "CREATE",
            "ALTER",
            "DROP",
        }:
            return None
        quote_pairs = {'"': '"', "`": "`", "[": "]"}
        for opener, closer in quote_pairs.items():
            if stripped.startswith(opener):
                end_idx = stripped.find(closer, 1)
                return stripped[1:end_idx] if end_idx != -1 else None
        match = re.match(r"^[A-Za-z_][\w$]*", stripped)
        return match.group(0) if match else None

    def process_regex_input(self, data):
        regex = data.split('"input.regex"')[1].split("=")[1]
        index = find_first_unpair_closed_par(regex)
        regex = regex[:index]
        data = data.replace(regex, " lexer_state_regex ")
        data = data.replace('"input.regex"', "parse_m_input_regex")
        self.lexer.state = {"lexer_state_regex": regex}
        return data

    def pre_process_data(self, data):
        data = cast(str, data.decode("utf-8"))
        # todo: not sure how to workaround ',' normal way
        if "input.regex" in data:
            data = self.process_regex_input(data)
        # Process the string character by character to handle quoted sections
        result = []
        in_quote = False
        in_line_comment = False
        in_block_comment = False
        i = 0
        symbol_spacing_map = {",", "(", ")"}

        # Special handling for odd number of single quotes
        if data.count("'") % 2 != 0:
            data = data.replace("\\'", "pars_m_single")

        while i < len(data):
            char = data[i]
            startswith = data[i:].startswith

            if in_line_comment:
                result.append(char)
                if startswith("\\n"):
                    in_line_comment = False
                    result.append("n")
                    i += 2
                else:
                    i += 1
                continue

            if in_block_comment:
                result.append(char)
                if startswith("*/"):
                    result.append("/")
                    in_block_comment = False
                    i += 2
                else:
                    i += 1
                continue

            if not in_quote and (startswith("--") or startswith("#")):
                in_line_comment = True
                result.append(char)
                i += 1
                continue

            if not in_quote and startswith("/*"):
                in_block_comment = True
                result.append(char)
                i += 1
                continue

            # Handle quote start/end
            if char == "'":
                in_quote = not in_quote
                result.append(char)

            # Handle line feeds in quotes
            elif in_quote and startswith("\\n"):
                result.append(LF_IN_QUOTE)
                i += 1

            # Handle equal sign in quotes
            elif in_quote and char == "=":
                result.append("\\03d")

            # Handle special unicode quotes
            elif not in_quote and (startswith(r"\u2018") or startswith(r"\u2019")):
                result.append("'")
                i += 5

            # Handle symbols that need spacing
            elif not in_quote and char in symbol_spacing_map:
                result.append(f" {char} ")

            # Keep all other characters as-is
            else:
                result.append(char)

            i += 1

        data = "".join(result)
        data = (
            data.replace("\\x", "\\0")
            .replace("'\\t'", "'pars_m_t'")
            .replace("\\t", " ")
        )
        return data

    def process_set(self) -> None:
        self.set_line = self.set_line.split()
        if self.set_line[-2] == "=":
            name = self.set_line[1]
        else:
            name = self.set_line[-2]
        value = self.set_line[-1].replace(";", "")
        self.tables.append({"name": name, "value": value})

    def parse_set_statement(self):
        if self.set_statement.match(self.line.upper()):
            self.set_was_in_line = True
            if not self.set_line:
                self.set_line = self.line
            else:
                self.process_set()
                self.set_line = self.line
        elif (self.set_line and len(self.set_line.split()) == 3) or (
            self.set_line and self.set_was_in_line
        ):
            self.process_set()
            self.set_line = None
            self.set_was_in_line = False

    def check_new_statement_start(self, line: str) -> bool:
        self.new_statement = False
        if self.statement and self.statement.count("(") == self.statement.count(")"):
            new_statements_tokens = ["ALTER ", "CREATE ", "DROP ", "SET "]
            for key in new_statements_tokens:
                if line.upper().startswith(key):
                    self.new_statement = True
        return self.new_statement

    def check_line_on_skip_words(self) -> bool:
        self.skip = False

        if self.skip_regex.match(self.line.upper()):
            self.skip = True
        return self.skip

    def add_line_to_statement(self) -> str:
        if (
            self.line
            and not self.skip
            and not self.set_was_in_line
            and not self.new_statement
        ):
            if self.statement is None:
                self.statement = self.line
            else:
                self.statement += f" {self.line}"

    def parse_data(self) -> List[Dict]:
        self.tables: List[Dict] = []
        data = self.pre_process_data(self.data)
        data = data.replace("\\t", "")
        lines = data.split("\\n")

        self.set_line: Optional[str] = None

        self.set_was_in_line: bool = False

        self.multi_line_comment = False
        self.multi_line_comment_collect_all = False

        for num, self.line in enumerate(lines):
            self.process_line(num != len(lines) - 1)
        self.resolve_create_table_as_select_statements()
        if self.comments:
            self.tables.append({"comments": self.comments})
        return self.tables

    @staticmethod
    def split_table_identifier(identifier: str) -> Tuple[Optional[str], str]:
        parts = [part.strip() for part in identifier.split(".") if part.strip()]
        if not parts:
            return None, identifier
        if len(parts) == 1:
            return None, parts[0]
        return parts[-2], parts[-1]

    @classmethod
    def parse_select_column_definition(
        cls, raw_column: str
    ) -> Optional[Dict[str, str]]:
        match = re.match(
            r"""
            ^\s*
            (?P<source>(?:[^\s,]+\.)?[^\s,]+)
            (?:\s+(?:AS\s+)?(?P<alias>[^\s,]+))?
            \s*$
            """,
            raw_column,
            flags=re.IGNORECASE | re.VERBOSE,
        )
        if not match:
            return None
        source_name = match.group("source").split(".")[-1]
        alias = match.group("alias")
        return {"source_name": source_name, "alias": alias}

    def parse_create_table_as_select_statement(self, statement: str) -> Optional[Dict]:
        match = CREATE_TABLE_AS_SELECT_RE.match(statement)
        if not match:
            return None

        target_schema, target_table_name = self.split_table_identifier(
            match.group("target")
        )
        source_schema, source_table_name = self.split_table_identifier(
            match.group("source")
        )
        raw_select = match.group("select").strip()
        if raw_select == "*":
            select_columns = "*"
        else:
            select_columns = []
            for raw_column in raw_select.split(","):
                parsed_column = self.parse_select_column_definition(raw_column.strip())
                if parsed_column is None:
                    return None
                select_columns.append(parsed_column)

        return {
            "__create_table_as_select__": {
                "schema": target_schema,
                "table_name": target_table_name,
                "source_schema": source_schema,
                "source_table_name": source_table_name,
                "select_columns": select_columns,
            }
        }

    def parse_create_view_statement(self, statement: str) -> Optional[Dict]:
        match = CREATE_VIEW_RE.match(statement)
        if not match:
            return None

        schema, view_name = self.split_table_identifier(match.group("target"))
        return {
            "schema": schema,
            "view_name": view_name,
            "definition": match.group("definition").strip(),
            "replace": bool(match.group("replace")),
        }

    def parse_drop_view_statement(self, statement: str) -> Optional[Dict]:
        match = DROP_VIEW_RE.match(statement)
        if not match:
            return None

        schema, view_name = self.split_table_identifier(match.group("target"))
        return {"schema": schema, "drop_view_name": view_name}

    @staticmethod
    def clone_create_table_as_select_columns(
        source_table: Dict, select_columns: Union[str, List[Dict[str, str]]]
    ) -> Optional[List[Dict]]:
        source_columns = {
            normalize_name(column["name"]): column
            for column in source_table.get("columns", [])
        }
        if select_columns == "*":
            return [deepcopy(column) for column in source_table.get("columns", [])]

        cloned_columns = []
        for selected_column in select_columns:
            source_column = source_columns.get(
                normalize_name(selected_column["source_name"])
            )
            if source_column is None:
                return None
            column_copy = deepcopy(source_column)
            if selected_column.get("alias"):
                column_copy["name"] = selected_column["alias"]
            cloned_columns.append(column_copy)
        return cloned_columns

    def build_create_table_as_select_statement(
        self, statement: Dict, source_table: Dict
    ) -> Optional[Dict]:
        data = statement["__create_table_as_select__"]
        columns = self.clone_create_table_as_select_columns(
            source_table, data["select_columns"]
        )
        if columns is None:
            return None
        return {
            "table_name": data["table_name"],
            "schema": data["schema"],
            "primary_key": [],
            "columns": columns,
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "tablespace": None,
        }

    def resolve_create_table_as_select_statements(self) -> None:
        unresolved = True
        while unresolved:
            unresolved = False
            tables_by_id = {
                get_table_id(table.get("schema"), table["table_name"]): table
                for table in self.tables
                if table.get("table_name") and "__create_table_as_select__" not in table
            }
            next_tables = []
            progress = False
            for statement in self.tables:
                create_table_as_select = statement.get("__create_table_as_select__")
                if create_table_as_select is None:
                    next_tables.append(statement)
                    continue
                source_table = tables_by_id.get(
                    get_table_id(
                        create_table_as_select["source_schema"],
                        create_table_as_select["source_table_name"],
                    )
                )
                if source_table is None:
                    unresolved = True
                    next_tables.append(statement)
                    continue
                resolved_statement = self.build_create_table_as_select_statement(
                    statement, source_table
                )
                if resolved_statement is None:
                    continue
                next_tables.append(resolved_statement)
                progress = True
            self.tables = next_tables
            if unresolved and not progress:
                self.tables = [
                    statement
                    for statement in self.tables
                    if "__create_table_as_select__" not in statement
                ]
                break

    def process_line(
        self,
        last_line: bool,
    ) -> Tuple[Optional[str], bool]:
        self.pre_process_line()

        # Remove whitespace, while preserving newlines in quotes
        self.line = self.line.strip().replace("\n", "").replace("\t", "")
        self.skip = self.check_line_on_skip_words()

        self.parse_set_statement()
        # to avoid issues when comma or parath are glued to column name
        self.check_new_statement_start(self.line)

        final_line = self.line.endswith(";") and not self.set_was_in_line

        self.add_line_to_statement()

        if (final_line or self.new_statement) and self.statement:
            # end of sql operation, remove ; from end of line
            self.statement = self.statement[:-1]
        elif last_line and not self.skip:
            # continue combine lines in one massive
            return

        self.set_default_flags_in_lexer()

        self.process_statement()

    def process_statement(self) -> None:
        if not self.set_line and self.statement:
            self.parse_statement()
        if self.new_statement:
            self.statement = self.line
            self.statement_inline_comments = []
        else:
            self.statement = None
            self.statement_inline_comments = []

    @staticmethod
    def normalize_generated_always_identity(statement: str) -> str:
        return re.sub(
            r"GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*\(",
            "GENERATED BY DEFAULT AS IDENTITY(",
            statement,
            flags=re.IGNORECASE,
        )

    @staticmethod
    def restore_generated_always_identity(statement: Dict) -> None:
        for column in statement.get("columns", []):
            generated_by = column.get("generated_by")
            if generated_by and generated_by.startswith("DEFAULT AS IDENTITY("):
                column["generated_by"] = generated_by.replace(
                    "DEFAULT AS IDENTITY(",
                    "ALWAYS AS IDENTITY(",
                    1,
                )

    def parse_statement(self) -> None:
        if any(regex.match(self.statement) for regex in self.skip_statement_regexes):
            return
        self.statement = self.strip_partition_definitions(self.statement)
        create_table_as_select_statement = self.parse_create_table_as_select_statement(
            self.statement
        )
        if create_table_as_select_statement:
            self.tables.append(create_table_as_select_statement)
            return
        create_view_statement = self.parse_create_view_statement(self.statement)
        if create_view_statement:
            self.tables.append(create_view_statement)
            return
        drop_view_statement = self.parse_drop_view_statement(self.statement)
        if drop_view_statement:
            self.tables.append(drop_view_statement)
            return
        _parse_result = self.yacc.parse(self.statement, lexer=self.lexer)
        if _parse_result:
            self.restore_range_bucket_partition_data(_parse_result)
            self.restore_mysql_index_prefix_lengths(_parse_result)
            if (
                _parse_result.get("if_exists")
                and _parse_result.get("table_name")
                and not getattr(self, "include_drop_statements", False)
            ):
                return
            if (
                self.has_generated_always_identity
                and not self.has_generated_by_default_identity
            ):
                self.restore_generated_always_identity(_parse_result)
            self.apply_inline_comments_to_statement(_parse_result)
            self.tables.append(_parse_result)

    def restore_range_bucket_partition_data(self, statement: Dict) -> None:
        partition_by = statement.get("partition_by")
        if (
            not isinstance(partition_by, dict)
            or partition_by.get("type") != "RANGE_BUCKET"
        ):
            return
        if partition_by.get("range"):
            return
        match = re.search(
            r"RANGE_BUCKET\s*\(\s*([^,\s()]+)\s*,\s*\[([^\]]+)\]\s*\)?",
            self.statement,
            flags=re.IGNORECASE,
        )
        if not match:
            return
        partition_by["columns"] = [match.group(1).strip()]
        partition_by["range"] = [
            item.strip() for item in match.group(2).split(",") if item.strip()
        ]

    def restore_mysql_index_prefix_lengths(self, statement: Dict) -> None:
        indexes = statement.get("index") or []
        if not indexes:
            return
        lengths_by_name = {}
        for match in re.finditer(
            r"(?:KEY|INDEX)\s+([^\s(]+)\s*\(\s*([^\s(]+)\s*\(\s*(\d+)\s*\)\s*\)",
            self.statement,
            flags=re.IGNORECASE,
        ):
            lengths_by_name[match.group(1)] = {
                "column": match.group(2),
                "length": int(match.group(3)),
            }
        if not lengths_by_name:
            return
        for index in indexes:
            name = index.get("index_name")
            if name not in lengths_by_name:
                continue
            detailed_columns = index.get("detailed_columns") or []
            if not detailed_columns:
                continue
            if detailed_columns[0].get("name") == lengths_by_name[name]["column"]:
                detailed_columns[0]["length"] = lengths_by_name[name]["length"]

    @staticmethod
    def normalize_inline_comment(comment: str) -> str:
        comment = " ".join(comment.strip().split())
        return comment.replace(" ,", ",")

    def apply_inline_comments_to_statement(self, statement: Dict) -> None:
        if (
            getattr(self, "output_mode", "sql") not in {"sql", "postgres", "psql"}
            or not self.statement_inline_comments
            or not isinstance(statement, dict)
        ):
            return
        columns = statement.get("columns") or []
        for item in self.statement_inline_comments:
            comment = self.normalize_inline_comment(item["comment"])
            if not comment:
                continue
            comment_name = normalize_name(item["name"])
            for column in columns:
                if normalize_name(column["name"]) == comment_name:
                    if not column.get("comment"):
                        column["comment"] = comment
                    break

    def set_default_flags_in_lexer(self) -> None:
        attrs = [
            "is_table",
            "sequence",
            "last_token",
            "columns_def",
            "after_columns",
            "check",
            "is_table",
            "last_par",
            "lp_open",
            "is_alter",
            "in_alter_column_definition",
            "is_like",
            "is_comment",
        ]
        for attr in attrs:
            setattr(self.lexer, attr, False)
        self.lexer.lt_open = 0

    def run(
        self,
        *,
        dump: bool = False,
        dump_path="schemas",
        file_path: Optional[str] = None,
        output_mode: str = "sql",
        group_by_type: bool = False,
        json_dump=False,
        custom_output_schema: Optional[Union[str, Callable]] = None,
    ) -> List[Dict]:
        """
        self.output_mode = output_mode
        dump: provide 'True' if you need to dump output in file
        dump_path: folder where you want to store result dump files
        file_path: pass full path to ddl file if you want to use this
            file name as name for the target output file
        output_mode: change output mode to get information relative to specific dialect,
            for example, in output_mode='hql' you will see also in self.tables such information as
            'external', 'stored_as', etc. Possible variants: ["mssql", "mysql", "oracle", "hql", "sql", "redshift"]
        group_by_type: if you set True, output will be formed as Dict with keys ['self.tables',
                'sequences', 'types', 'domains']
            and each dict will contain list of parsed entities. Without it output is a List with Dicts where each
            Dict == one entity from ddl - one table or sequence or type.
        custom_output_schema: custom output schema name or callable to reshape output, for example "bigquery".
        """
        if (
            isinstance(custom_output_schema, str)
            and custom_output_schema.lower() == "bigquery"
            and output_mode == "sql"
        ):
            output_mode = "bigquery"
        if output_mode not in dialect_by_name:
            raise SimpleDDLParserException(
                f"Output mode can be one of possible variants: {dialect_by_name.keys()}"
            )
        self.include_drop_statements = bool(file_path)
        self.tables = self.parse_data()
        if file_path:
            self.normalize_file_comment_output()
        self.tables = Output(
            parser_output=self.tables,
            group_by_type=group_by_type,
            output_mode=output_mode,
        ).format()
        if custom_output_schema:
            from simple_ddl_parser.output.custom_schemas import (
                apply_custom_output_schema,
            )

            self.tables = apply_custom_output_schema(custom_output_schema, self.tables)
        if dump:
            if file_path:
                # if we run parse from one file - save same way to one file
                dump_data_to_file(
                    os.path.basename(file_path).split(".")[0], dump_path, self.tables
                )
            else:
                for table in self.tables:
                    dump_data_to_file(table["table_name"], dump_path, table)
        if json_dump:
            self.tables = json.dumps(self.tables)
        return self.tables

    def normalize_file_comment_output(self) -> None:
        for item in self.tables:
            comments = item.get("comments")
            if not comments:
                continue
            item["comments"] = [
                (
                    comment[:-4] + " "
                    if isinstance(comment, str)
                    and comment.startswith("!")
                    and comment.endswith(" */;")
                    else comment
                )
                for comment in comments
            ]
