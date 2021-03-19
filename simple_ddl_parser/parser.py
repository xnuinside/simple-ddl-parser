import os
from ply import lex, yacc
from typing import Dict, List, Optional, Tuple
from simple_ddl_parser.output import dump_data_to_file, result_format


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
        self.data = content
        self.paren_count = 0
        self.lexer = lex.lex(object=self, debug=False)
        self.yacc = yacc.yacc(module=self, debug=False)

    @staticmethod
    def pre_process_line(line: str, block_comments: List[str]) -> Tuple[str, List]:
        OP_COM = '/*'
        CL_COM = '*/'
        IN_COM = '--'
        MYSQL_COM = '#'
        code_line = ""
        
        if line.strip().startswith(MYSQL_COM) or line.strip().startswith(IN_COM):
            return code_line, block_comments
         
        if CL_COM not in line and OP_COM not in line and IN_COM not in line:
            return line, block_comments
        if IN_COM in line:
            code_line = line.split(IN_COM)[0]
        if OP_COM in line:
            code_line += line.split(OP_COM)[0]
            block_comments.append(OP_COM)
        if CL_COM in code_line and block_comments:
            block_comments.pop(-1)
            code_line += code_line.split(CL_COM)[1]
        return code_line, block_comments
                    
    def parse_data(self):
        tables = []
        table = []
        block_comments = []
        statement = None
        for line in self.data.split("\n"):
            line, block_comments = self.pre_process_line(line, block_comments)
            if line.replace("\n", "").replace("\t", ""):
                # to avoid issues when comma are glued to column name
                line = line.replace(",", " , ").replace("(", " ( ").replace(")", " ) ")
                if "CREATE" in line.upper() and "TABLE" in line.upper():
                    statement = line
                elif statement != None:
                    statement += f" {line}"
                elif "TABLE" not in line.upper() and not statement:
                    statement = line
                else:
                    statement = line
                if ";" not in statement:
                    continue
                _parse_result = yacc.parse(statement)
                if _parse_result:
                    table.append(_parse_result)
                if line.strip().endswith(";"):
                    if table:
                        tables.append(table)
                    table = []
                statement = None
        return tables

    def run(
        self, *, dump=None, dump_path="schemas", file_path: Optional[str] = None
    ) -> List[Dict]:
        """ run parser """
        tables = self.parse_data()
        tables = result_format(tables)
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
