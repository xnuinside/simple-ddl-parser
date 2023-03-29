from simple_ddl_parser import DDLParser


def test_inherits():

    ddl = """
    CREATE TABLE public."Diagnosis_identifier" (
        "Diagnosis_id" text NOT NULL
    )
    INHERITS (public.identifier);
    """

    result = DDLParser(ddl).run()

    expected = [{'alter': {},
  'checks': [],
  'columns': [{'check': None,
               'default': None,
               'name': '"Diagnosis_id"',
               'nullable': False,
               'references': None,
               'size': None,
               'type': 'text',
               'unique': False}],
  'index': [],
  'inherits': {'schema': 'public', 'table_name': 'identifier'},
  'partitioned_by': [],
  'primary_key': [],
  'schema': 'public',
  'table_name': '"Diagnosis_identifier"',
  'tablespace': None}]
    assert expected == result
