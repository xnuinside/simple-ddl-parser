import os
import re
from typing import Dict, List, Optional, Tuple

from ply import lex, yacc

from simple_ddl_parser.output.common import dump_data_to_file, result_format
from simple_ddl_parser.utils import find_first_unpair_closed_par


class Parser:
    """
    Base class for a lexer/parser that has the rules defined as methods

        It could not be loaded or called without Subclass,

        for example: DDLParser

        Subclass must include tokens for parser and rules
    """

    def __init__(self, content) -> None:
        """ init parser for file """
        self.tables = []
        self.data = content.encode("unicode_escape")
        self.paren_count = 0
        self.lexer = lex.lex(object=self, debug=False)
        self.yacc = yacc.yacc(module=self, debug=False)
        self.columns_closed = False

    @staticmethod
    def pre_process_line(line: str, block_comments: List[str]) -> Tuple[str, List]:
        OP_COM = "/*"
        CL_COM = "*/"
        IN_COM = "--"
        MYSQL_COM = "#"
        code_line = ""
        comma_only_str = r"((\')|(' ))+(,)((\')|( '))+\B"
        line = re.sub(comma_only_str, "_ddl_parser_comma_only_str", line)
        if "(" not in line:
            line = line.replace("<", " < ").replace(">", " > ")
        if line.strip().startswith(MYSQL_COM) or line.strip().startswith(IN_COM):
            return code_line, block_comments

        if IN_COM in line:
            if re.search(r"((\")|(\'))+(.)*(--)+(.)*((\")|(\'))+", line):
                return line, block_comments
            code_line = line.split(IN_COM)[0]
        elif CL_COM not in line and OP_COM not in line:
            return line, block_comments
        if OP_COM in line:
            code_line += line.split(OP_COM)[0]
            block_comments.append(OP_COM)
        if CL_COM in code_line and block_comments:
            block_comments.pop(-1)
            code_line += code_line.split(CL_COM)[1]
        return code_line, block_comments

    def pre_process_data(self, data):
        data = data.decode("utf-8")
        # todo: not sure how to workaround ',' normal way
        if "input.regex" in data:
            regex = data.split('"input.regex"')[1].split("=")[1]
            index = find_first_unpair_closed_par(regex)
            regex = regex[:index]
            data = data.replace(regex, " lexer_state_regex ")
            data = data.replace('"input.regex"', "parse_m_input_regex")
            self.lexer.state = {"lexer_state_regex": regex}

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
        )
        return data

    def parse_data(self):
        tables = []
        table = []
        block_comments = []
        statement = None
        data = self.pre_process_data(self.data)
        lines = data.replace("\\t", "").split("\\n")
        for num, line in enumerate(lines):
            line, block_comments = self.pre_process_line(line, block_comments)
            if line.replace("\n", "").replace("\t", "") or num == len(lines) - 1:
                # to avoid issues when comma or parath are glued to column name
                if statement is not None:
                    statement += f" {line}"
                else:
                    statement = line
                if ";" not in statement and num != len(lines) - 1:
                    continue
                self.set_default_flags_in_lexer()
                _parse_result = yacc.parse(statement)

                if _parse_result:
                    table.append(_parse_result)
                if line.strip().endswith(";") or num == len(lines) - 1:
                    if table:
                        tables.append(table)
                    table = []
                statement = None
        return tables

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
    ) -> List[Dict]:
        """
        dump: provide 'True' if you need to dump output in file
        dump_path: folder where you want to store result dump files
        file_path: pass full path to ddl file if you want to use this
            file name as name for the target output file
        output_mode: change output mode to get information relative to specific dialect,
            for example, in output_mode='hql' you will see also in tables such information as
            'external', 'stored_as', etc. Possible variants: ["mssql", "mysql", "oracle", "hql", "sql"]
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
        return tables
