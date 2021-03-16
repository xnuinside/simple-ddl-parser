
Simple DDL Parser
-----------------


.. image:: https://img.shields.io/pypi/v/simple-ddl-parser
   :target: https://img.shields.io/pypi/v/simple-ddl-parser
   :alt: badge1
 
.. image:: https://img.shields.io/pypi/l/simple-ddl-parser
   :target: https://img.shields.io/pypi/l/simple-ddl-parser
   :alt: badge2
 
.. image:: https://img.shields.io/pypi/pyversions/simple-ddl-parser
   :target: https://img.shields.io/pypi/pyversions/simple-ddl-parser
   :alt: badge3
 

Build with ply (lex & yacc in python). A lot of samples in 'tests/'

How to install
^^^^^^^^^^^^^^

.. code-block:: bash


       pip install simple-ddl-parser

How to use
----------

From python code
^^^^^^^^^^^^^^^^

.. code-block:: python

       from simple_ddl_parser import DDLParser


       parse_results = DDLParser("""create table dev.data_sync_history(
           data_sync_id bigint not null,
           sync_count bigint not null,
           sync_mark timestamp  not  null,
           sync_start timestamp  not null,
           sync_end timestamp  not null,
           message varchar(2000) null,
           primary key (data_sync_id, sync_start)
       ); """).run()

       print(parse_results)

To parse from file
^^^^^^^^^^^^^^^^^^

.. code-block:: python


       from simple_ddl_parser import parse_from_file

       result = parse_from_file('tests/sql/test_one_statement.sql')
       print(result)

From command line
^^^^^^^^^^^^^^^^^

simple-ddl-parser is installed to environment as command **sdp**

.. code-block:: bash


       sdp path_to_ddl_file

       # for example:

       sdp tests/sql/test_two_tables.sql

You will see the output in **schemas** folder in file with name **test_two_tables_schema.json**

If you want to have also output in console - use **-v** flag for verbose.

.. code-block:: bash


       sdp tests/sql/test_two_tables.sql -v

If you don't want to dump schema in file and just print result to the console, use **--no-dump** flag:

.. code-block:: bash


       sdp tests/sql/test_two_tables.sql --no-dump

You can provide target path where you want to dump result with argument **-t**\ , **--targer**\ :

.. code-block:: bash


       sdp tests/sql/test_two_tables.sql -t dump_results/

How does it work?
^^^^^^^^^^^^^^^^^

Parser tested on different DDLs for PostgreSQL & Hive.
Types that are used in your DB does not matter, so parser must also work successfuly to any DDL for SQL DB. Parser is NOT case sensitive, it did not expect that all queries will be in upper case or lower case. So you can write statements like this:

.. code-block:: sql

   Alter Table Persons ADD CONSTRAINT CHK_PersonAge CHECK (Age>=18 AND City='Sandnes');

It will be parsed as is without errors.

If you have samples that cause an error - please open the issue (but don't forget to add ddl example), I will be glad to fix it.

A lot of statements and output result you can find in tests, for example:

`test_alter_statements.py <tests/test_alter_statements.py>`_ 

This parser take as input SQL DDL statements or files, for example like this:

.. code-block:: sql


       CREATE TABLE employees (
           id SERIAL PRIMARY KEY,
           first_name VARCHAR (50),
           last_name VARCHAR (50),
           birth_date DATE CHECK (birth_date > '1900-01-01'),
           joined_date DATE CHECK (joined_date > birth_date),
           salary numeric CHECK(salary > 0),
           phone_numbers varchar(16) array,
           tags varchar ARRAY[1]
       );
       CREATE TABLE dev.Persons (
           ID int NOT NULL,
           LastName varchar(255) NOT NULL,
           FirstName varchar(255),
           Age int,
           City varchar(255),
           Country varchar(255),
           CONSTRAINT CHK_Person CHECK (Age>=19 AND City='Sandnes')
       );

       ALTER TABLE dev.Persons ADD CHECK (Age>=18 AND City='Sandnes');

       ALTER TABLE dev.Persons Add CONSTRAINT ck_person  CHECK (Age>=18 AND City='Sandnes');
       Alter Table dev.Persons ADD CONSTRAINT fk_group FOREIGN KEY (id) REFERENCES employees (id); 
       create unique index person_pk on dev.Persons (ID) ;
       create index person_ix2 on dev.Persons (City, Country);

And produce output like this (information about table name, schema, columns, types and properties):

.. code-block:: python

   [{
     'table_name': 'employees',
     'index': [],
     'primary_key': ['id'],
     'schema': None,
     'alter': {},
     'checks': [],
     'columns': [{'check': None,
                  'default': None,
                  'name': 'id',
                  'nullable': False,
                  'references': None,
                  'size': None,
                  'type': 'SERIAL',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'first_name',
                  'nullable': True,
                  'references': None,
                  'size': 50,
                  'type': 'VARCHAR',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'last_name',
                  'nullable': True,
                  'references': None,
                  'size': 50,
                  'type': 'VARCHAR',
                  'unique': False},
                 {'check': "birth_date > '1900-01-01'",
                  'default': None,
                  'name': 'birth_date',
                  'nullable': True,
                  'references': None,
                  'size': None,
                  'type': 'DATE',
                  'unique': False},
                 {'check': 'joined_date > birth_date',
                  'default': None,
                  'name': 'joined_date',
                  'nullable': True,
                  'references': None,
                  'size': None,
                  'type': 'DATE',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'phone_numbers',
                  'nullable': True,
                  'references': None,
                  'size': 16,
                  'type': 'varchar[]',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'tags',
                  'nullable': True,
                  'references': None,
                  'size': None,
                  'type': 'varchar[1]',
                  'unique': False}]},

       {'table_name': 'Persons',
       'index': [{'columns': ['ID'], 'index_name': 'person_pk', 'unique': True},
                   {'columns': ['City', 'Country'],
                   'index_name': 'person_ix2',
                   'unique': False}],
       'primary_key': [],
       'schema': 'dev',
       'alter': {'checks': [{'constraint_name': None,
                           'statement': ['Age>=18', 'AND', "City='Sandnes'"]},
                          {'constraint_name': 'ck_person',
                           'statement': ['Age>=18', 'AND', "City='Sandnes'"]}],
               'columns': [{'constraint_name': 'fk_group',
                            'name': 'id',
                            'references': {'column': 'id',
                                           'schema': None,
                                           'table': 'employees'}}]},
     'checks': [{'constraint_name': 'CHK_Person',
                 'statement': "Age>=19 AND City='Sandnes'"}],
     'columns': [{'check': None,
                  'default': None,
                  'name': 'ID',
                  'nullable': False,
                  'references': None,
                  'size': None,
                  'type': 'int',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'LastName',
                  'nullable': False,
                  'references': None,
                  'size': 255,
                  'type': 'varchar',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'FirstName',
                  'nullable': True,
                  'references': None,
                  'size': 255,
                  'type': 'varchar',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'Age',
                  'nullable': True,
                  'references': None,
                  'size': None,
                  'type': 'int',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'City',
                  'nullable': True,
                  'references': None,
                  'size': 255,
                  'type': 'varchar',
                  'unique': False},
                 {'check': None,
                  'default': None,
                  'name': 'Country',
                  'nullable': True,
                  'references': None,
                  'size': 255,
                  'type': 'varchar',
                  'unique': False}]
                  }]

SEQUENCES
^^^^^^^^^

When we parse SEQUENCES each property stored as a separate dict KEY, for example for sequence:

.. code-block:: sql

       CREATE SEQUENCE dev.incremental_ids
       INCREMENT 1
       START 1
       MINVALUE 1
       MAXVALUE 9223372036854775807
       CACHE 1;

Will be output:

.. code-block:: python

       [
           {'schema': 'dev', 'incremental_ids': 'document_id_seq', 'increment': 1, 'start': 1, 'minvalue': 1, 'maxvalue': 9223372036854775807, 'cache': 1}
       ]

ALTER statements
^^^^^^^^^^^^^^^^

Right now added support only for ALTER statements with FOREIGEIN key

For example, if in your ddl after table defenitions (create table statements) you have ALTER table statements like this:

.. code-block:: sql


   ALTER TABLE "material_attachments" ADD FOREIGN KEY ("material_id", "material_title") REFERENCES "materials" ("id", "title");

This statements will be parsed and information about them putted inside 'alter' key in table's dict.
For example, please check alter statement tests - **tests/test_alter_statements.py**

More examples & tests
^^^^^^^^^^^^^^^^^^^^^

You can find in **tests/** folder.

Dump result in json
^^^^^^^^^^^^^^^^^^^

To dump result in json use argument .run(dump=True)

You also can provide a path where you want to have a dumps with schema with argument .run(dump_path='folder_that_use_for_dumps/')

Supported Statements
^^^^^^^^^^^^^^^^^^^^


* 
  CREATE TABLE [ IF NOT EXISTS ] + columns defenition, columns attributes: column name + type + type size(for example, varchar(255)), UNIQUE, PRIMARY KEY, DEFAULT, CHECK, NULL/NOT NULL, REFERENCES

* 
  STATEMENTS: PRIMARY KEY, CHECK, FOREIGN KEY in table defenitions (in create table();)

* 
  ALTER TABLE STATEMENTS: ADD CHECK (with CONSTRAINT), ADD FOREIGN KEY (with CONSTRAINT)

TODO in next Releases (if you don't see feature that you need - open the issue)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


#. Provide API to get result as Python Object
#. Add online demo (UI) to parse ddl

Historical context
^^^^^^^^^^^^^^^^^^

This library is an extracted parser code from https://github.com/xnuinside/fakeme (Library for fake relation data generation, that I used in several work projects, but did not have time to make from it normal open source library)

For one of the work projects I needed to convert SQL ddl to Python ORM models in auto way and I tried to use https://github.com/andialbrecht/sqlparse but it works not well enough with ddl for my case (for example, if in ddl used lower case - nothing works, primary keys inside ddl are mapped as column name not reserved word and etc.).
So I remembered about Parser in Fakeme and just extracted it & improved. 

How to run tests
^^^^^^^^^^^^^^^^

.. code-block:: bash


       git clone https://github.com/xnuinside/simple-ddl-parser.git
       cd simple-ddl-parser
       poetry install # if you use poetry
       # or use `pip install .`
       pytest tests/ -vv

How to contribute
-----------------

Please describe issue that you want to solve and open the PR, I will review it as soon as possible.

Any questions? Ping me in Telegram: https://t.me/xnuinside 

Changelog
---------

**v0.7.0**


#. Redone logic of parse CREATE TABLE statements, now they parsed as one statement (not line by line as previous)
#. Fixed several minor bugs with edge cases in default values and checks
#. Added support for ALTER FOREIGN KEY statement for several fields in one statement

**v0.6.1**


#. Fix minor bug with schema in index statements

**v0.6.0**


#. Added support for SEQUENCE statemensts
#. Added support for ARRAYs in types
#. Added support for CREATE INDEX statements

**v0.5.0**


#. Added support for UNIQUE column attribute
#. Add command line arg to pass folder with ddls (parse multiple files)
#. Added support for CHECK Constratint
#. Added support for FOREIGN Constratint in ALTER TABLE

**v0.4.0**


#. Added support schema for table in REFERENCES statement in column defenition
#. Added base support fot Alter table statements (added 'alters' key in table)
#. Added command line arg to pass path to get the output results
#. Fixed incorrect null fields parsing

**v0.3.0**


#. Added support for REFERENCES statement in column defenition
#. Added command line
