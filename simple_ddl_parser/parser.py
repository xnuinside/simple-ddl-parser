import json
import os
import re
from typing import Dict, List, Optional, Tuple

from ply import lex, yacc

from simple_ddl_parser.output.common import dump_data_to_file, result_format
from simple_ddl_parser.utils import find_first_unpair_closed_par

OP_COM = "/*"
CL_COM = "*/"

IN_COM = "--"
MYSQL_COM = "#"


class Parser:
    """
    Base class for a lexer/parser that has the rules defined as methods

        It could not be loaded or called without Subclass,

        for example: DDLParser

        Subclass must include tokens for parser and rules
    """

    def __init__(self, content: str) -> None:
        """init parser for file"""
        self.tables = []
        self.data = content.encode("unicode_escape")
        self.paren_count = 0
        self.lexer = lex.lex(object=self, debug=False)
        self.yacc = yacc.yacc(module=self, debug=False)
        self.columns_closed = False
        self.statement = None
        self.block_comments = []
        self.comments = []

    def pre_process_line(self) -> Tuple[str, List]:
        code_line = ""
        comma_only_str = r"((\')|(' ))+(,)((\')|( '))+\B"
        self.line = re.sub(comma_only_str, "_ddl_parser_comma_only_str", self.line)

        if not (
            self.line.strip().startswith(MYSQL_COM)
            or self.line.strip().startswith(IN_COM)
        ):
            code_line = self.process_inline_comments(code_line)
        self.line = code_line

    def process_in_comment(self, line: str) -> str:
        if re.search(r"((\")|(\'))+(.)*(--)+(.)*((\")|(\'))+", line):
            code_line = line
        else:
            splitted_line = line.split(IN_COM)
            code_line = splitted_line[0]
            self.comments.append(splitted_line[1])
        return code_line

    def previous_comment_processing(self) -> str:
        code_line = ""
        if IN_COM in self.line:
            code_line = self.process_in_comment(self.line)
        elif CL_COM not in self.line and OP_COM not in self.line:
            code_line = self.line
        return code_line

    def process_inline_comments(self, code_line: str) -> Tuple[str, List]:
        comment = None
        code_line = self.previous_comment_processing()
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

        data = (
            data.replace(",", " , ")
            .replace("(", " ( ")
            .replace(")", " ) ")
            .replace("\\x", "\\0")
            .replace("‘", "'")
            .replace("’", "'")
            .replace("\\u2018", "'")
            .replace("\\u2019", "'")
            .replace("'\\t'", "'pars_m_t'")
            .replace("'\\n'", "'pars_m_n'")
            .replace("\\'", "pars_m_single")
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
        if re.match(r"SET", self.line.upper()):
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
        skip_line_words = ["USE", "GO"]

        self.skip = False
        for word in skip_line_words:
            if self.line.startswith(word):
                self.skip = True
                break
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

    def parse_data(self):
        self.tables: List[Dict] = []
        data = self.pre_process_data(self.data)
        lines = data.replace("\\t", "").split("\\n")

        self.set_line: Optional[str] = None

        self.set_was_in_line: bool = False

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

        if final_line or self.new_statement:
            # end of sql operation, remove ; from end of line
            self.statement = self.statement[:-1]
        elif last_line and not self.skip:
            # continue combine lines in one massive
            return

        self.set_default_flags_in_lexer()

        self.process_statement()

    def process_statement(self):
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

    def set_default_flags_in_lexer(self):
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
        self.tables = self.parse_data()
        self.tables = result_format(self.tables, output_mode, group_by_type)
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
