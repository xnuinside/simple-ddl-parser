import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple, Union

from ply import lex, yacc

from simple_ddl_parser.exception import SimpleDDLParserException
from simple_ddl_parser.output.core import Output, dump_data_to_file
from simple_ddl_parser.output.dialects import dialect_by_name
from simple_ddl_parser.utils import find_first_unpair_closed_par

# open comment
OP_COM = "/*"
# close comment
CL_COM = "*/"

IN_COM = "--"
MYSQL_COM = "#"


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

        # self.comma_only_str = re.compile(r"((\')|(' ))+(,)((\')|( '))+\B")
        self.equal_without_space = re.compile(r"(\b)=")
        self.in_comment = re.compile(r"((\")|(\'))+(.)*(--)+(.)*((\")|(\'))+")
        self.set_statement = re.compile(r"SET ")
        self.skip_regex = re.compile(r"^(GO|USE|INSERT|GRANT|DELETE)\b")

    def catch_comment_or_process_line(self, code_line: str) -> str:
        if self.multi_line_comment:
            self.comments.append(self.line)
            if CL_COM in self.line:
                self.multi_line_comment = False
            return ""

        elif not (
            self.line.strip().startswith(MYSQL_COM)
            or self.line.strip().startswith(IN_COM)
        ):
            return self.process_inline_comments(code_line)
        return code_line

    def pre_process_line(self) -> Tuple[str, List]:
        code_line = ""
        # self.line = self.comma_only_str.sub("_ddl_parser_comma_only_str", self.line)
        self.line = self.equal_without_space.sub(" = ", self.line)
        code_line = self.catch_comment_or_process_line(code_line)
        if self.line.startswith(OP_COM) and CL_COM not in self.line:
            self.multi_line_comment = True
        elif self.line.startswith(CL_COM):
            self.multi_line_comment = False
        self.line = code_line

    def process_in_comment(self, line: str) -> str:
        if self.in_comment.search(line):
            code_line = line
        else:
            splitted_line = line.split(IN_COM)
            code_line = splitted_line[0]
            self.comments.append(splitted_line[1])
        return code_line

    def process_line_before_comment(self) -> str:
        """get useful codeline - remove comment"""
        code_line = ""
        if IN_COM in self.line:
            code_line = self.process_in_comment(self.line)
        elif CL_COM not in self.line and OP_COM not in self.line:
            code_line = self.line
        return code_line

    def process_inline_comments(self, code_line: str) -> Tuple[str, List]:
        """this method сatches comments like "create table ( # some comment" - inline this statement"""
        comment = None
        code_line = self.process_line_before_comment()
        if OP_COM in self.line:
            splitted_line = self.line.split(OP_COM)
            code_line += splitted_line[0]
            comment = splitted_line[1]
            self.block_comments.append(OP_COM)
        if CL_COM in code_line and self.block_comments:
            splitted_line = self.line.split(CL_COM)
            self.block_comments.pop(-1)
            code_line += splitted_line[1]
            comment = splitted_line[0]

        if comment:
            self.comments.append(comment)
        return code_line

    def process_regex_input(self, data):
        regex = data.split('"input.regex"')[1].split("=")[1]
        index = find_first_unpair_closed_par(regex)
        regex = regex[:index]
        data = data.replace(regex, " lexer_state_regex ")
        data = data.replace('"input.regex"', "parse_m_input_regex")
        self.lexer.state = {"lexer_state_regex": regex}
        return data

    def pre_process_data(self, data):
        data = data.decode("utf-8")
        # todo: not sure how to workaround ',' normal way
        if "input.regex" in data:
            data = self.process_regex_input(data)
        quote_before = r"((?!\'[\w]*[\\']*[\w]*)"
        quote_after = r"((?![\w]*[\\']*[\w]*\')))"
        num = 0
        # add space everywhere except strings
        for symbol, replace_to in [
            (r"(,)+", " , "),
            (r"((\()){1}", " ( "),
            (r"((\))){1}", " ) "),
        ]:
            num += 1
            if num == 2:
                # need for correct work with `(`` but not need in other symbols
                quote_after_use = quote_after.replace(")))", "))*)")
            else:
                quote_after_use = quote_after
            data = re.sub(quote_before + symbol + quote_after_use, replace_to, data)

        if data.count("'") % 2 != 0:
            data = data.replace("\\'", "pars_m_single")
        data = (
            data.replace("\\x", "\\0")
            .replace("‘", "'")
            .replace("’", "'")
            .replace("\\u2018", "'")
            .replace("\\u2019", "'")
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
        regex_n = r"((?!\'[\w]*[\\']*[\w]*)\\n(?![\w]*[\\']*[\w]*\'))"
        data = data.replace("\\t", "")
        lines = re.split(regex_n, data)
        lines = [line for line in lines if line != "\\n"]

        self.set_line: Optional[str] = None

        self.set_was_in_line: bool = False

        self.multi_line_comment = False

        for num, self.line in enumerate(lines):
            self.process_line(num != len(lines) - 1)
        if self.comments:
            self.tables.append({"comments": self.comments})
        return self.tables

    def process_line(
        self,
        last_line: bool,
    ) -> Tuple[Optional[str], bool]:
        self.pre_process_line()

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
        else:
            self.statement = None

    def parse_statement(self) -> None:
        _parse_result = yacc.parse(self.statement)
        if _parse_result:
            self.tables.append(_parse_result)

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
            "is_like",
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
    ) -> List[Dict]:
        """
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
        """
        if output_mode not in dialect_by_name:
            raise SimpleDDLParserException(
                f"Output mode can be one of possible variants: {dialect_by_name.keys()}"
            )
        self.tables = self.parse_data()
        self.tables = Output(
            parser_output=self.tables,
            group_by_type=group_by_type,
            output_mode=output_mode,
        ).format()
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
