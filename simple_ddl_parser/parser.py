import os
from ply import lex, yacc
from typing import Dict, List, Optional
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

    def parse_data(self):
        tables = []
        table = []
        # todo: need to re-done this and parse table defenitions same way till ;
        is_table = False
        statement = None
        for line in self.data.split("\n"):
            if line.replace("\n", "").replace("\t", ""):
                if "CREATE" in line.upper() and "TABLE" in line.upper():
                    is_table = True
                    statement = line
                elif statement != None and not is_table:
                    statement += f" {line}"
                elif "TABLE" not in line.upper() and not statement:
                    statement = line
                else:
                    statement = line
                if ";" not in statement and not is_table:
                    continue
                if ";" in line and is_table:
                    is_table = False
                _parse_result = yacc.parse(statement)
                self.sequence = False
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
        """ run lex and yacc on prepared data from files """
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
