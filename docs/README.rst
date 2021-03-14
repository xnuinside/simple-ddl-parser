
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


       create table prod.super_table
   (
       data_sync_id bigint not null default 0,
       id_ref_from_another_table int REFERENCES another_table (id)
       sync_count bigint not null REFERENCES count_table (count),
       sync_mark timestamp  not  null,
       sync_start timestamp  not null default now(),
       sync_end timestamp  not null,
       message varchar(2000) null,
       primary key (data_sync_id, sync_start)
   );

And produce output like this (information about table name, schema, columns, types and properties):

.. code-block:: python


       [
           {
               "columns": [
                   {
                       "name": "data_sync_id", "type": "bigint", "size": None, 
                       "nullable": False, "default": None, "references": None,
                   },
                   {
                       "name": "id_ref_from_another_table", "type": "int", "size": None,
                       "nullable": False, "default": None, "references": {"table": "another_table", "schema": None, "column": "id"},
                   },
                   {
                       "name": "sync_count", "type": "bigint", "size": None,
                       "nullable": False, "default": None, "references": {"table": "count_table", "schema": None, "column": "count"},
                   },
                   {
                       "name": "sync_mark", "type": "timestamp", "size": None,
                       "nullable": False, "default": None, "references": None,
                   },
                   {
                       "name": "sync_start", "type": "timestamp", "size": None,
                       "nullable": False, "default": None, "references": None,
                   },
                   {
                       "name": "sync_end", "type": "timestamp", "size": None,
                       "nullable": False, "default": None, "references": None,
                   },
                   {
                       "name": "message", "type": "varchar", "size": 2000,
                       "nullable": False, "default": None, "references": None,
                   },
               ],
               "primary_key": ["data_sync_id", "sync_start"],
               "table_name": "super_table",
               "schema": "prod",
               "alter": {}
           }
       ]

Or one more example

.. code-block:: sql


   CREATE TABLE "paths" (
     "id" int PRIMARY KEY,
     "title" varchar NOT NULL,
     "description" varchar(160),
     "created_at" timestamp,
     "updated_at" timestamp
   );

and result

.. code-block:: python

           [{
           'columns': [
               {'name': 'id', 'type': 'int', 'nullable': False, 'size': None, 'default': None, 'references': None}, 
               {'name': 'title', 'type': 'varchar', 'nullable': False, 'size': None, 'default': None, 'references': None}, 
               {'name': 'description', 'type': 'varchar', 'nullable': False, 'size': 160, 'default': None, 'references': None}, 
               {'name': 'created_at', 'type': 'timestamp', 'nullable': False, 'size': None, 'default': None, 'references': None}, 
               {'name': 'updated_at', 'type': 'timestamp', 'nullable': False, 'size': None, 'default': None, 'references': None}], 
           'primary_key': ['id'], 
           'table_name': 'paths', 
           'schema': None,
           'alter': {}
           }]

If you pass file or text block with more when 1 CREATE TABLE statement when result will be list of such dicts. For example:

Input:

.. code-block:: sql


   CREATE TABLE "countries" (
     "id" int PRIMARY KEY,
     "code" varchar(4) NOT NULL,
     "name" varchar NOT NULL
   );

   CREATE TABLE "path_owners" (
     "user_id" int,
     "path_id" int,
     "type" int DEFAULT 1
   );

Output:

.. code-block:: python


       [
           {'columns': [
               {'name': 'id', 'type': 'int', 'size': None, 'nullable': False, 'default': None, 'references': None}, 
               {'name': 'code', 'type': 'varchar', 'size': 4, 'nullable': False, 'default': None, 'references': None}, 
               {'name': 'name', 'type': 'varchar', 'size': None, 'nullable': False, 'default': None, 'references': None}], 
            'primary_key': ['id'], 
            'table_name': 'countries', 
            'schema': None}, 
           {'columns': [
               {'name': 'user_id', 'type': 'int', 'size': None, 'nullable': False, 'default': None, 'references': None}, 
               {'name': 'path_id', 'type': 'int', 'size': None, 'nullable': False, 'default': None, 'references': None}, 
               {'name': 'type', 'type': 'int', 'size': None, 'nullable': False, 'default': 1, 'references': None}], 
            'primary_key': [], 
            'table_name': 'path_owners', 
            'schema': None,
            'alter': {}}
       ]

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


#. CREATE TABLE [ IF NOT EXISTS ]
#. columns defenition, columns attributes:

    2.0 column name + type + type size(for example, varchar(255))

    2.1 UNIQUE

    2.2 PRIMARY KEY

    2.3 DEFAULT

    2.4 CHECK

    2.5 NULL/NOT NULL

    2.6 REFERENCES

#. PRRIMARY KEY, CHECK, FOREIGN KEY in table defenitions (in create table();)

#. ALTER TABLE:

    4.1 ADD CHECK (with CONSTRAINT)

    4.2 ADD FOREIGN KEY (with CONSTRAINT)

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

**v0.6.0** (not released, current master)


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
