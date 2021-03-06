import os
import re
import json
from typing import Dict, List
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
        'NUM_VALUE_SDP': 'NUM_VALUE_SDP',
        'PRIMARY': 'PRIMARY',
        'KEY': 'KEY', 
        'DEFAULT': 'DEFAULT'
    }

    tokens = tuple(['ID',
                    'NEWLINE',
                    'DOT'] + list(reserved.values()))

    t_ignore = '\t<>();, \'"${}\r'
    t_DOT= r'.'
    
    def t_NUM_VALUE_SDP(self, t):
        r'[0-9]+\D'
        t.type = "NUM_VALUE_SDP"
        t.value = t.value.replace(')', '')
        print(t.value)
        print(t.type)
        return t

    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z_0-9:]*'
        print(t.value)
        t.type = self.reserved.get(t.value.upper(), 'ID')  # Check for reserved word
        print(t.type)
        return t

    def t_newline(self, t):
        r'\n+'
        self.lexer.lineno += len(t.value)
        t.type = "NEWLINE"
        print(t.type)
        if self.lexer.paren_count == 0:
            return t

    def t_error(self, t):
        raise SyntaxError("Unknown symbol %r" % (t.value[0],))

    def p_error(self, p):
        print('errrrror')
        pass

    def p_expression_table_name(self, p):
        """expr : table ID DOT ID
                | table ID     
        """
        # get schema & table name
        p_list = list(p)
        
        schema = None
        if len(p) > 4:
            if '.' in p:
                schema = p_list[-3]
                table_name = p_list[-1]
        else:
            table_name = p_list[-1]
        p[0] = {'schema': schema, 'table_name': table_name}
    
    def p_ttable(self, p):
        """table : CREATE TABLE IF NOT EXISTS
                | CREATE TABLE
                
        """
        # get schema & table name
        pass
    def p_column(self, p):
        """column : ID ID
                  | ID ID NUM_VALUE_SDP
                  | ID NUM_VALUE_SDP
        """
        print('p_expression_column_type')
        size = None
        print([x for x in p])
        type_str = p[2]
        if len(p) == 4:
            match = re.match(r'[0-9]+', p[3])
            if bool(match):
                size =  int(p[3])
        p[0] = {"name": p[1], 
                "type": type_str, 
                'size': size}
        
    def p_defcolumn(self, p):
        """ expr : column
                | column DEFAULT NUM_VALUE_SDP
                | column NOT NULL
                | column NULL
                | column PRIMARY KEY
                | column DEFAULT ID
        """
        print('p_expression_type')
        pk = False
        nullable = False
        default = None
        print([x for x in p])
        p[0] = p[1]
        print([x for x in p])
        if 'KEY' in p and 'PRIMARY' in p:
            pk = True
        if 'NULL' in p and 'NOT' not in p:
            nullable = True
        if 'DEFAULT' in p:
            ind_default = list(p).index('DEFAULT')
            default = p[ind_default+1]
            if default.isnumeric():
                default = int(default)
        p[0].update({"nullable": nullable, "primary_key": pk, "default": default})
    def p_expression_primary_key(self, p):
        # todo: need to redone id lists
        """expr : PRIMARY KEY ID
                | PRIMARY KEY ID ID
                | PRIMARY KEY ID ID ID
                | PRIMARY KEY ID ID ID ID
                | PRIMARY KEY ID ID ID ID ID
        """
        p[0] = {'primary_key': [x for x in p[3:] if x != ',']}
        
    def dump_schema(self, table_name, dump_path):
        """ method to dump json schema """
        if not os.path.isdir(dump_path):
            os.makedirs(dump_path, exist_ok=True)
        with open("{}/{}_schema.json".format(dump_path, table_name),
                  'w+') as schema_file:
            json.dump(self.result, schema_file, indent=1)
    
    def result_format(self, result: List[Dict]) -> List[Dict]:
        final_result = []
        for table in result:
            table_data = {'columns': [], 'primary_key': None}
            for item in table:
                if item.get('table_name'):
                    table_data['table_name'] = item['table_name']
                    table_data['schema'] = item['schema']
                elif not item.get('type') and item.get('primary_key'):
                    table_data['primary_key'] = item['primary_key']
                else:
                    table_data['columns'].append(item)
            if not table_data['primary_key']:
                table_data = self.check_pk_in_columns(table_data)
            else:
                table_data = self.remove_pk_from_columns(table_data)
            final_result.append(table_data)
        return final_result
    
    @staticmethod
    def remove_pk_from_columns(table_data: Dict):
        for column in table_data['columns']:
            del column['primary_key']
        return table_data
    
    @staticmethod
    def check_pk_in_columns(table_data: Dict):
        pk = []
        for column in table_data['columns']:
            if column['primary_key']:
                pk.append(column['name'])
            del column['primary_key']
        table_data['primary_key'] = pk
        return table_data

    def run(self, *, dump=None, dump_path="schemas", lower_case=False):
        """ run lex and yacc on prepared data from files """
        result = super().run()
        print(result)
        table_data = self.result_format(result)
        print(table_data)
        if dump:
            self.dump_schema(table_data['table_name'], dump_path)
        return table_data

def parse_from_file(file_path: str, **kwargs):
    """ get useful data from ddl """
    with open(file_path, 'r') as df:
        return DDLParser(df.read()).run(**kwargs)

ddl = """

CREATE TABLE "countries" (
  "id" int PRIMARY KEY,
  "code" varchar(4) NOT NULL,
  "name" varchar NOT NULL
);

CREATE TABLE "path_owners" (
  "user_id" int,
  "path_id" int,
  "type" int DEFAULT 1a
);


"""

print(DDLParser(ddl).run())