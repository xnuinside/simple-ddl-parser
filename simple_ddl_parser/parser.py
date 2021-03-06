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
    
    def run(self):
        """ run lex and yacc on prepared data from files """
        for line in self.data.split("\n"):
            if line.replace("\n", "").replace("\t", ""):
                _parse_result = yacc.parse(line)
                if _parse_result:
                    self.result.append(_parse_result)
        return self.result
