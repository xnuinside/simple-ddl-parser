from ply import lex, yacc


class Parser:
    """
    Base class for a lexer/parser that has the rules defined as methods

        It could not be loaded or called without Subclass, 
        
        for example: DDLParser

        Subclass must include tokens for parser and rules
    """
    def __init__(self, content) -> None:
        """ init parser for file """
        self.result = []
        self.data = content
        self.paren_count = 0
        self.lexer = lex.lex(object=self, debug=True)
        self.yacc = yacc.yacc(module=self, debug=True)
    
    def prepare_data(self):
        data, final_data = [], []
        last_elem, first_elem = None, None
        table_creating = None
        for line in self.data.split('\n'):
            if line.startswith("--"):
                # remove comments line from file data
                continue
            if 'SORTED' not in line and 'CLUSTERED' not in line and (
                    line.startswith("(") or line.startswith("\n(")
                    or line.replace("\n", "").endswith("(")):
                first_elem = len(data)
            if "ALTER" in line and "(" in line:
                line = line.split("(")[1].split(")")[0].replace(",", ",\n")
            if 'ASC' not in line and line.replace("\n", "").startswith(")"):
                last_elem = len(data)
            elif line.startswith("CREATE"):
                table_creating = line
            data.append(line)
        if table_creating:
            final_data.append(table_creating)
        if final_data:
            [final_data.append(line) for line in data[first_elem + 1:last_elem]]
        result_data = "".join(final_data)
        return result_data

    def run(self):
        """ run lex and yacc on prepared data from files """
        for line in self.data.split("\n"):
            if line.replace("\n", "").replace("\t", ""):
                _parse_result = yacc.parse(line)
                if _parse_result:
                    self.result.append(_parse_result)
        return self.result
