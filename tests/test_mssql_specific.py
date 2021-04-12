from simple_ddl_parser import DDLParser


def test_int_identity_type():
    ddl = """
    CREATE TABLE sqlserverlist (

    id INT IDENTITY (1,1) PRIMARY KEY, -- NOTE THE IDENTITY (1,1) IS SIMILAR TO serial in postgres - Format for IDENTITY [ (seed , increment) ]
    company_id BIGINT ,
    )
    """

    result = DDLParser(ddl).run(group_by_type=True)
    expected = {'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'id',
                          'nullable': False,
                          'references': None,
                          'size': (1, 1),
                          'type': 'INT IDENTITY',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'company_id',
                          'nullable': True,
                          'references': None,
                          'size': None,
                          'type': 'BIGINT',
                          'unique': False}],
             'index': [],
             'partitioned_by': [],
             'primary_key': ['id'],
             'schema': None,
             'table_name': 'sqlserverlist'}],
 'types': []}
    assert expected == result


def test_mssql_foreign_ref_in_column():
        
    ddl = """
    CREATE TABLE sqlserverlist (

    id INT IDENTITY (1,1) PRIMARY KEY, -- NOTE THE IDENTITY (1,1) IS SIMILAR TO serial in postgres - Format for IDENTITY [ (seed , increment) ]
    company_id BIGINT ,
    primary_id INT FOREIGN KEY REFERENCES Persons(PersonID), -- ADD THIS COLUMN FOR THE FOREIGN KEY
    )
    """

    result = DDLParser(ddl).run(group_by_type=True)
    expected = {'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'id',
                          'nullable': False,
                          'references': None,
                          'size': (1, 1),
                          'type': 'INT IDENTITY',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'company_id',
                          'nullable': True,
                          'references': None,
                          'size': None,
                          'type': 'BIGINT',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'primary_id',
                          'nullable': True,
                          'references': {'column': 'PersonID',
                                         'deferrable_initially': None,
                                         'on_delete': None,
                                         'on_update': None,
                                         'schema': None,
                                         'table': 'Persons'},
                          'size': None,
                          'type': 'INT',
                          'unique': False}],
             'index': [],
             'partitioned_by': [],
             'primary_key': ['id'],
             'schema': None,
             'table_name': 'sqlserverlist'}],
 'types': []}
    assert expected == result


def test_max_supported_as_column_size():
    ddl = """
    CREATE TABLE sqlserverlist (

    user_account VARCHAR(8000) NOT NULL,
    user_first_name VARCHAR(max) NOT NULL,
    )
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'user_account',
                          'nullable': False,
                          'references': None,
                          'size': 8000,
                          'type': 'VARCHAR',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'user_first_name',
                          'nullable': False,
                          'references': None,
                          'size': 'max',
                          'type': 'VARCHAR',
                          'unique': False}],
             'index': [],
             'partitioned_by': [],
             'primary_key': [],
             'schema': None,
             'table_name': 'sqlserverlist'}],
 'types': []}
    assert expected == result

def test_constraint_unique():
    ddl = """
    CREATE TABLE sqlserverlist (

    id INT IDENTITY (1,1) PRIMARY KEY, -- NOTE THE IDENTITY (1,1) IS SIMILAR TO serial in postgres - Format for IDENTITY [ (seed , increment) ]
    company_id BIGINT ,
    user_last_name 	VARBINARY(8000) NOT NULL,
    CONSTRAINT UC_sqlserverlist_last_name UNIQUE (company_id,user_last_name)
    )
    """

    result = DDLParser(ddl).run(group_by_type=True)
    expected = {'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'id',
                          'nullable': False,
                          'references': None,
                          'size': (1, 1),
                          'type': 'INT IDENTITY',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'company_id',
                          'nullable': True,
                          'references': None,
                          'size': None,
                          'type': 'BIGINT',
                          'unique': True},
                         {'check': None,
                          'default': None,
                          'name': 'user_last_name',
                          'nullable': False,
                          'references': None,
                          'size': 8000,
                          'type': 'VARBINARY',
                          'unique': True}],
             'index': [],
             'partitioned_by': [],
             'primary_key': ['id'],
             'schema': None,
             'table_name': 'sqlserverlist',
             'constraints': {'unique' : {'columns': ['company_id', 'user_last_name'],
                                   'name': 'UC_sqlserverlist_last_name'}}}],
 'types': []}

    assert expected == result


def test_constraint_unique_none():
    ddl = """
    CREATE TABLE sqlserverlist (

    id INT IDENTITY (1,1) PRIMARY KEY, -- NOTE THE IDENTITY (1,1) IS SIMILAR TO serial in postgres - Format for IDENTITY [ (seed , increment) ]
    company_id BIGINT ,
    user_last_name 	VARBINARY(8000) NOT NULL
    )
    """

    result = DDLParser(ddl).run(group_by_type=True, output_mode='mssql')
    expected = {'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'id',
                          'nullable': False,
                          'references': None,
                          'size': (1, 1),
                          'type': 'INT IDENTITY',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'company_id',
                          'nullable': True,
                          'references': None,
                          'size': None,
                          'type': 'BIGINT',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'user_last_name',
                          'nullable': False,
                          'references': None,
                          'size': 8000,
                          'type': 'VARBINARY',
                          'unique': False}],
             'index': [],
             'partitioned_by': [],
             'primary_key': ['id'],
             'schema': None,
             'table_name': 'sqlserverlist',
             'constraints': {
                'unique': None, 
                'check': None, 
                'references': None}}],
             'types': []}

    assert expected == result
    