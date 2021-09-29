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
        """ init parser for file """
        self.tables = []
        self.data = content.encode("unicode_escape")
        self.paren_count = 0
        self.lexer = lex.lex(object=self, debug=False)
        self.yacc = yacc.yacc(module=self, debug=False)
        self.columns_closed = False

    def pre_process_line(
        self, line: str, block_comments: List[str]
    ) -> Tuple[str, List]:
        code_line = ""
        comma_only_str = r"((\')|(' ))+(,)((\')|( '))+\B"
        line = re.sub(comma_only_str, "_ddl_parser_comma_only_str", line)
        if "(" not in line:
            line = line.replace("<", " < ").replace(">", " > ")

        if not (line.strip().startswith(MYSQL_COM) or line.strip().startswith(IN_COM)):
            code_line, block_comments = self.process_inline_comments(
                line, code_line, block_comments
            )
        return code_line, block_comments

    @staticmethod
    def process_in_comment(line: str):
        if re.search(r"((\")|(\'))+(.)*(--)+(.)*((\")|(\'))+", line):
            code_line = line
        else:
            code_line = line.split(IN_COM)[0]
        return code_line

    def process_inline_comments(
        self, line: str, code_line: str, block_comments: List
    ) -> Tuple[str, List]:
        if IN_COM in line:
            code_line = self.process_in_comment(line)
        elif CL_COM not in line and OP_COM not in line:
            code_line = line
        if OP_COM in line:
            code_line += line.split(OP_COM)[0]
            block_comments.append(OP_COM)
        if CL_COM in code_line and block_comments:
            block_comments.pop(-1)
            code_line += code_line.split(CL_COM)[1]
        return code_line, block_comments

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

    def parse_data(self):
        tables = []
        block_comments = []
        statement = None
        data = self.pre_process_data(self.data)
        lines = data.replace("\\t", "").split("\\n")
        for num, line in enumerate(lines):

            line, block_comments = self.pre_process_line(line, block_comments)

            line = line.strip().replace("\n", "").replace("\t", "")

            if line or num == len(lines) - 1:
                # to avoid issues when comma or parath are glued to column name
                final_line = line.strip().endswith(";")
                if statement is None:
                    statement = line
                else:
                    statement += f" {line}"

                if final_line:
                    # end of sql operation, remove ; from end of line
                    statement = statement[:-1]
                elif num != len(lines) - 1:
                    # continue combine lines in one massive
                    continue

                self.set_default_flags_in_lexer()

                self.parse_statement(tables, statement)

                statement = None
        return tables

    @staticmethod
    def parse_statement(tables: List, statement: str):
        _parse_result = yacc.parse(statement)

        if _parse_result:
            tables.append(_parse_result)

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
            for example, in output_mode='hql' you will see also in tables such information as
            'external', 'stored_as', etc. Possible variants: ["mssql", "mysql", "oracle", "hql", "sql", "redshift"]
        group_by_type: if you set True, output will be formed as Dict with keys ['tables',
                'sequences', 'types', 'domains']
            and each dict will contain list of parsed entities. Without it output is a List with Dicts where each
            Dict == one entity from ddl - one table or sequence or type.
        """
        tables = self.parse_data()
        tables = result_format(tables, output_mode, group_by_type)
        if dump:
            if file_path:
                # if we run parse from one file - save same way to one file
                dump_data_to_file(
                    os.path.basename(file_path).split(".")[0], dump_path, tables
                )
            else:
                for table in tables:
                    dump_data_to_file(table["table_name"], dump_path, table)
        if json_dump:
            tables = json.dumps(tables)
        return tables
