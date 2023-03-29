from simple_ddl_parser import DDLParser


def test_in_tablespace():

    ddl = """
        CREATE TABLE TEST.CRM_JOB_PARAM (
    COL1 VARCHAR(50) NOT NULL,
    COl2 VARCHAR(50),
    COL3 VARCHAR(50) DEFAULT '0',
    COL3 TIMESTAMP
    )
    IN TABLESPACE1


    """

    result = DDLParser(ddl).run()
    expected = [{'alter': {},
  'checks': [],
  'columns': [{'check': None,
               'default': None,
               'name': 'COL1',
               'nullable': False,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': None,
               'name': 'COl2',
               'nullable': True,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': "'0'",
               'name': 'COL3',
               'nullable': True,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': None,
               'name': 'COL3',
               'nullable': True,
               'references': None,
               'size': None,
               'type': 'TIMESTAMP',
               'unique': False}],
  'index': [],
  'partitioned_by': [],
  'primary_key': [],
  'schema': 'TEST',
  'table_name': 'CRM_JOB_PARAM',
  'tablespace': 'TABLESPACE1'}]
    assert result == expected


def test_index_in():

    ddl = """
        CREATE TABLE TEST.CRM_JOB_PARAM (
    COL1 VARCHAR(50) NOT NULL,
    COl2 VARCHAR(50),
    COL3 VARCHAR(50) DEFAULT '0',
    COL3 TIMESTAMP
    )
    IN TABLESPACE1
    INDEX IN TABLESPACE2

    """

    result = DDLParser(ddl).run()
    expected = [{'alter': {},
  'checks': [],
  'columns': [{'check': None,
               'default': None,
               'name': 'COL1',
               'nullable': False,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': None,
               'name': 'COl2',
               'nullable': True,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': "'0'",
               'name': 'COL3',
               'nullable': True,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': None,
               'name': 'COL3',
               'nullable': True,
               'references': None,
               'size': None,
               'type': 'TIMESTAMP',
               'unique': False}],
  'index': [],
  'index_in': 'TABLESPACE2',
  'partitioned_by': [],
  'primary_key': [],
  'schema': 'TEST',
  'table_name': 'CRM_JOB_PARAM',
  'tablespace': 'TABLESPACE1'}]
    assert result == expected


def test_organize_by_row():

    ddl = """
        CREATE TABLE TEST.CRM_JOB_PARAM (
    COL1 VARCHAR(50) NOT NULL,
    COl2 VARCHAR(50),
    COL3 VARCHAR(50) DEFAULT '0',
    COL3 TIMESTAMP
    )
    ORGANIZE BY ROw
    """

    result = DDLParser(ddl).run()
    expected = [{'alter': {},
  'checks': [],
  'columns': [{'check': None,
               'default': None,
               'name': 'COL1',
               'nullable': False,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': None,
               'name': 'COl2',
               'nullable': True,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': "'0'",
               'name': 'COL3',
               'nullable': True,
               'references': None,
               'size': 50,
               'type': 'VARCHAR',
               'unique': False},
              {'check': None,
               'default': None,
               'name': 'COL3',
               'nullable': True,
               'references': None,
               'size': None,
               'type': 'TIMESTAMP',
               'unique': False}],
  'index': [],
  'organize_by': 'ROW',
  'partitioned_by': [],
  'primary_key': [],
  'schema': 'TEST',
  'table_name': 'CRM_JOB_PARAM',
  'tablespace': None}]
    
    assert expected == result
