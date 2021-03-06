import os
import re
import json
from simple_ddl_parser.parser import Parser


class DDLParser(Parser):
    """
        lex and yacc parser for parse ddl into BQ schemas
    """
    reserved = {
        'IF': 'IF',
        'then': 'THEN',
        'else': 'ELSE',
        'while': 'WHILE',
        'USE': 'USE',
        'CREATE': 'CREATE',
        'TABLE': 'TABLE',
        'NOT': 'NOT',
        'EXISTS': 'EXISTS',
        'NULL': 'NULL',
        'INT': 'INT',
        'PRIMARY': 'PRIMARY',
        'KEY': 'KEY'
    }

    tokens = tuple(['COMMA',
                    'ID',
                    'NEWLINE',
                    'DOT'] + list(reserved.values()))

    t_ignore = '\t\n<>();, ${}\r'
    t_COMMA = r'\,'
    t_DOT= r'.'
    
    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z_0-9:]*'
        t.type = self.reserved.get(t.value.upper(), 'ID')  # Check for reserved word
        return t

    def t_int(self, t):
        r'[0-9]+\D'
        t.type = "INT"
        t.value = t.value.replace(')', '')
        return t

    def t_newline(self, t):
        r'\n+'
        self.lexer.lineno += len(t.value)
        t.type = "NEWLINE"
        if self.lexer.paren_count == 0:
            return t

    def t_error(self, t):
        raise SyntaxError("Unknown symbol %r" % (t.value[0],))

    def p_error(self, p):
        pass

    def p_expression_table_name(self, p):
        """expr : CREATE TABLE IF NOT EXISTS ID DOT ID 
                | CREATE TABLE IF NOT EXISTS ID
                | CREATE TABLE ID
                | CREATE TABLE ID DOT ID
                
        """
        # get schema & table name
        if len(p) > 4:
            if p[4] == '.':
                schema = p[3]
                table_name = p[5]
        else:
            schema = ''
            table_name = p[3]
        p[0] = {'schema': schema, 'table_name': table_name}

    def p_expression_type(self, p):
        """expr : ID ID
                | ID ID NOT NULL
                | ID ID NULL
                | ID ID INT NULL 
                | ID ID INT NOT NULL 
                | ID
        """
        nullable = False
        size = None
        print([x for x in p])
        if len(p) == 2:
            # type unknown
            type_str = 'STRING'
        else:
            type_str = p[2]
        if len(p) >= 4:
            match = re.match(r'[0-9]+', p[3])
            print(match)
            if bool(match):
                print('size')
                print(size)
                size =  int(p[3])
            elif p[3] == 'NULL':
                nullable = True
        p[0] = {"name": p[1], "type": type_str, "mode": nullable, "size": size}
    
    def p_expression_primary_key(self, p):
        # todo: need to redone id lists
        """expr : PRIMARY KEY ID
                | PRIMARY KEY ID ID
                | PRIMARY KEY ID ID ID
                | PRIMARY KEY ID ID ID ID 
                | PRIMARY KEY ID ID ID ID ID
                | PRIMARY KEY ID ID ID ID ID ID
        """
        p[0] = {'primary_key': [x for x in p[3:] if x != ',']}
        
    def dump_schema(self, table_name, dump_path):
        """ method to dump json schema """
        if not os.path.isdir(dump_path):
            os.makedirs(dump_path, exist_ok=True)
        with open("{}/{}_schema.json".format(dump_path, table_name),
                  'w+') as schema_file:
            json.dump(self.result, schema_file, indent=1)
    
    def result_format(self, result, lower_case):
        table_data = {'columns': []}
        for item in result:
            if item.get('table_name'):
                table_data['table_name'] = item['table_name'].lower() if lower_case else item['table_name'] 
                table_data['schema'] = item['schema'].lower() if lower_case else item['schema']
            elif item.get('primary_key'):
                if not lower_case:
                    table_data['primary_key'] = item['primary_key']
                else:
                    table_data['primary_key'] = [x.lower() for x in item['primary_key']]
            else:
                if not lower_case:
                    table_data['columns'].append(item)
                else:
                    # todo: add lower case to columns
                    table_data['columns'].append(item)
        return table_data
    
    def run(self, *, dump=None, dump_path="schemas", lower_case=False):
        """ run lex and yacc on prepared data from files """
        result = super().run()
        table_data = self.result_format(result, lower_case)
        if dump:
            self.dump_schema(table_data['table_name'], dump_path)
        return table_data

def parse_from_file(file_path: str):
    """ get useful data from ddl """
    with open(file_path, 'r') as df:
        return DDLParser(df.read())
