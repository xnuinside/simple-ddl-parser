
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
 
.. image:: https://github.com/xnuinside/simple-ddl-parser/actions/workflows/main.yml/badge.svg
   :target: https://github.com/xnuinside/simple-ddl-parser/actions/workflows/main.yml/badge.svg
   :alt: workflow


Build with ply (lex & yacc in python). A lot of samples in 'tests/.

Is it Stable?
^^^^^^^^^^^^^

Yes, library already has about 7000+ downloads per day - https://pypistats.org/packages/simple-ddl-parser.

As maintainer, I guarantee that any backward incompatible changes will not be done in patch or minor version. Only additionals & new features.

However, in process of adding support for new statements & features I see that output can be structured more optimal way and I hope to release version ``1.0.*`` with more struct output result. But, it will not be soon, first of all, I want to add support for so much statements as I can. So I don't think make sense to expect version 1.0.* before, for example, version ``0.26.0`` :)

How does it work?
^^^^^^^^^^^^^^^^^

Parser tested on different DDLs mostly for PostgreSQL & Hive. But idea to support as much as possible DDL dialects (AWS 
Redshift, Oracle, Hive, MsSQL, etc.). You can check dialects sections after ``Supported Statements`` section to get more information that statements from dialects already supported by parser.

Feel free to open Issue with DDL sample
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**If you need some statement, that not supported by parser yet**\ : please provide DDL example & information about that is it SQL dialect or DB.

Types that are used in your DB does not matter, so parser must also work successfuly to any DDL for SQL DB. Parser is NOT case sensitive, it did not expect that all queries will be in upper case or lower case. So you can write statements like this:

.. code-block:: sql


       Alter Table Persons ADD CONSTRAINT CHK_PersonAge CHECK (Age>=18 AND City='Sandnes');

It will be parsed as is without errors.

If you have samples that cause an error - please open the issue (but don't forget to add ddl example), I will be glad to fix it.

A lot of statements and output result you can find in tests on the github - https://github.com/xnuinside/simple-ddl-parser/tree/main/tests .

How to install
^^^^^^^^^^^^^^

.. code-block:: bash


       pip install simple-ddl-parser

How to use
----------

Extract additional information from HQL (& other dialects)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In some dialects like HQL there is a lot of additional information about table like, fore example, is it external table, STORED AS, location & etc. This propertie will be always empty in 'classic' SQL DB like PostgreSQL or MySQL and this is the reason, why by default this information are 'hidden'.
Also some fields hidden in HQL, because they are simple not exists in HIVE, for example 'deferrable_initially'
To get this 'hql' specific details about table in output please use 'output_mode' argument in run() method.

example:

.. code-block:: python


       ddl = """
       CREATE TABLE IF NOT EXISTS default.salesorderdetail(
           SalesOrderID int,
           ProductID int,
           OrderQty int,
           LineTotal decimal
           )
       PARTITIONED BY (batch_id int, batch_id2 string, batch_32 some_type)
       LOCATION 's3://datalake/table_name/v1'
       ROW FORMAT DELIMITED
           FIELDS TERMINATED BY ','
           COLLECTION ITEMS TERMINATED BY '\002'
           MAP KEYS TERMINATED BY '\003'
       STORED AS TEXTFILE
       """

       result = DDLParser(ddl).run(output_mode="hql")
       print(result)

And you will get output with additional keys 'stored_as', 'location', 'external', etc.

.. code-block:: python


       # additional keys examples
     {
       ...,
       'location': "'s3://datalake/table_name/v1'",
       'map_keys_terminated_by': "'\\003'",
       'partitioned_by': [{'name': 'batch_id', 'size': None, 'type': 'int'},
                           {'name': 'batch_id2', 'size': None, 'type': 'string'},
                           {'name': 'batch_32', 'size': None, 'type': 'some_type'}],
       'primary_key': [],
       'row_format': 'DELIMITED',
       'schema': 'default',
       'stored_as': 'TEXTFILE',
       ... 
     }

If you run parser with command line add flag '-o=hql' or '--output-mode=hql' to get the same result.

Possible output_modes: ["mssql", "mysql", "oracle", "hql", "sql", "redshift", "snowflake"]

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

More details
^^^^^^^^^^^^

``DDLParser(ddl).run()``
.run() method contains several arguments, that impact changing output result. As you can saw upper exists argument ``output_mode`` that allow you to set dialect and get more fields in output relative to chosen dialect, for example 'hql'. Possible output_modes: ["mssql", "mysql", "oracle", "hql", "sql"]

Also in .run() method exists argument ``group_by_type`` (by default: False). By default output of parser looks like a List with Dicts where each dict == one entitiy from ddl (table, sequence, type, etc). And to understand that is current entity you need to check Dict like: if 'table_name' in dict - this is a table, if 'type_name' - this is a type & etc.

To make work little bit easy you can set group_by_type=True and you will get output already sorted by types, like:

.. code-block:: python


       { 
           'tables': [all_pasrsed_tables], 
           'sequences': [all_pasrsed_sequences], 
           'types': [all_pasrsed_types], 
           'domains': [all_pasrsed_domains],
           ...
       }

For example:

.. code-block:: python


       ddl = """
       CREATE TYPE "schema--notification"."ContentType" AS
           ENUM ('TEXT','MARKDOWN','HTML');
           CREATE TABLE "schema--notification"."notification" (
               content_type "schema--notification"."ContentType"
           );
       CREATE SEQUENCE dev.incremental_ids
           INCREMENT 10
           START 0
           MINVALUE 0
           MAXVALUE 9223372036854775807
           CACHE 1;
       """

       result = DDLParser(ddl).run(group_by_type=True)

       # result will be:

       {'sequences': [{'cache': 1,
                       'increment': 10,
                       'maxvalue': 9223372036854775807,
                       'minvalue': 0,
                       'schema': 'dev',
                       'sequence_name': 'incremental_ids',
                       'start': 0}],
       'tables': [{'alter': {},
                   'checks': [],
                   'columns': [{'check': None,
                               'default': None,
                               'name': 'content_type',
                               'nullable': True,
                               'references': None,
                               'size': None,
                               'type': '"schema--notification"."ContentType"',
                               'unique': False}],
                   'index': [],
                   'partitioned_by': [],
                   'primary_key': [],
                   'schema': '"schema--notification"',
                   'table_name': '"notification"'}],
       'types': [{'base_type': 'ENUM',
                   'properties': {'values': ["'TEXT'", "'MARKDOWN'", "'HTML'"]},
                   'schema': '"schema--notification"',
                   'type_name': '"ContentType"'}]}

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
--------------------


* 
  CREATE TABLE [ IF NOT EXISTS ] + columns defenition, columns attributes: column name + type + type size(for example, varchar(255)), UNIQUE, PRIMARY KEY, DEFAULT, CHECK, NULL/NOT NULL, REFERENCES, ON DELETE, ON UPDATE,  NOT DEFERRABLE, DEFERRABLE INITIALLY, GENERATED ALWAYS, STORED, COLLATE

* 
  STATEMENTS: PRIMARY KEY, CHECK, FOREIGN KEY in table defenitions (in create table();)

* 
  ALTER TABLE STATEMENTS: ADD CHECK (with CONSTRAINT), ADD FOREIGN KEY (with CONSTRAINT), ADD UNIQUE, ADD DEFAULT FOR

* 
  PARTITION BY statement

* 
  CREATE SEQUENCE with words: INCREMENT, START, MINVALUE, MAXVALUE, CACHE

* 
  CREATE TYPE statement:  AS ENUM, AS OBJECT, INTERNALLENGTH, INPUT, OUTPUT

* 
  LIKE statement (in this and only in this case to output will be added 'like' keyword with information about table from that we did like - 'like': {'schema': None, 'table_name': 'Old_Users'}).

* 
  TABLESPACE statement

* 
  COMMENT ON statement

* 
  CREATE SCHEMA [IF NOT EXISTS] ... [AUTHORIZATION] ...

* 
  CREATE DOMAIN [AS]

* 
  CREATE [SMALLFILE | BIGFILE] [TEMPORARY] TABLESPACE statement

* 
  CREATE DATABASE + Properties parsing

HQL Dialect statements
^^^^^^^^^^^^^^^^^^^^^^


* PARTITIONED BY statement
* ROW FORMAT, ROW FORMAT SERDE
* WITH SERDEPROPERTIES ("input.regex" =  "..some regex..")
* STORED AS (AVRO, PARQUET, etc), STORED AS INPUTFORMAT, OUTPUTFORMAT
* COMMENT
* LOCATION
* FIELDS TERMINATED BY, LINES TERMINATED BY, COLLECTION ITEMS TERMINATED BY, MAP KEYS TERMINATED BY
* TBLPROPERTIES ('parquet.compression'='SNAPPY' & etc.)
* SKEWED BY

MSSQL / MySQL/ Oracle
^^^^^^^^^^^^^^^^^^^^^


* type IDENTITY statement
* FOREIGN KEY REFERENCES statement
* 'max' specifier in column size
* CONSTRAINT ... UNIQUE, CONSTRAINT ... CHECK, CONSTRAINT ... FOREIGN KEY, CONSTRAINT ... PRIMARY KEY
* CREATE CLUSTERED INDEX

Oracle
^^^^^^


* ENCRYPT column property [+ NO SALT, SALT, USING]
* STORAGE column property

AWS Redshift Dialect statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


* ENCODE column property
* SORTKEY, DISTSTYLE, DISTKEY, ENCODE table properties
* 
  CREATE TEMP / TEMPORARY TABLE

* 
  syntax like with LIKE statement:

  ``create temp table tempevent(like event);``

Snowflake Dialect statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^


* CREATE .. CLONE statements for table, database and schema
* CREATE TABLE .. CLUSTER BY ..
* CONSTRAINT .. [NOT] ENFORCED 

TODO in next Releases (if you don't see feature that you need - open the issue)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^




#. Add support for ALTER TABLE ... ADD COLUMN
#. Add more support for CREATE type IS TABLE (example: CREATE OR REPLACE TYPE budget_tbl_typ IS TABLE OF NUMBER(8,2);
#. Add support (ignore correctly) ALTER TABLE ... DROP CONSTRAINT ..., ALTER TABLE ... DROP INDEX ...

non-feature todo
----------------


#. Provide API to get result as Python Object
#. Add online demo (UI) to parse ddl

Thanks for involving & contributions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Big thanks for the involving & contribution with test cases with DDL samples & opening issues goes to:


* https://github.com/kukigai , 
* https://github.com/Awalkman90 ,
* https://github.com/geob3d

Changelog
---------

**v0.19.8**
Features:

.. code-block::

   1. Method `DDLParser(...).run(...)` now get argument json=True if you want to get result as json,
   but not as Python Object


Fixes:

.. code-block::

   1. Fixed issue when variables are 'glue' during Struct parse like previously STRUCT<a ARRAY<STRING>,b BOOL> was
   extracted like 'STRUCT <aARRAY <STRING>,bBOOL>', now this issue was fixed and it parsed as is STRUCT < a
   ARRAY < STRING > ,b BOOL >. Now '>' and '<' always will be with space near them.

   2. CHECK CONSTRAINT with functions. Fix for https://github.com/xnuinside/simple-ddl-parser/issues/76.



**v0.19.7**
Fixes:


#. Add support for more special symbols to strings - https://github.com/xnuinside/simple-ddl-parser/issues/68

Features:


#. Added support for HQL statements:
    STORED AS INPUTFORMAT, OUTPUTFORMAT - https://github.com/xnuinside/simple-ddl-parser/issues/69
    SKEWED BY

**v0.19.6**
Fixes:


#. Fixed issue with PARTITIONED BY multiple columns in HQL - https://github.com/xnuinside/simple-ddl-parser/issues/66
#. Question symbol '?' now handled valid in strings - https://github.com/xnuinside/simple-ddl-parser/issues/64
#. Fixed issue with escaping symbols & added tests -https://github.com/xnuinside/simple-ddl-parser/issues/63

Features:


#. Added support for HQL statement TBLPROPERTIES - https://github.com/xnuinside/simple-ddl-parser/issues/65

**v0.19.5**
Fixes:


#. Fixed issues with COMMENT statement in column definitions. Add bunch of tests, now they expect working ok.

**v0.19.4**


#. Added support for PARTITION BY (previously was only PARTITIONED BY from HQL)

**v0.19.2**


#. Added support for ` quotes in column & tables names

**v0.19.1**
Fixes:


#. Issue with '\t' reported in https://github.com/xnuinside/simple-ddl-parser/issues/53

Features:


#. Added base for future BigQuery support: added output_mode="bigquery". Pay attention that there is no schemas in BigQuery, so schemas are Datasets.

**v0.19.0**
**Features**


#. Added support for base Snowflake SQL Dialect.
   Added new --output-mode='snowflake' (add "clone" key)

Added support for CREATE .. CLONE with same behaviour as CREATE .. LIKE
Added support for CREATE .. CLONE for schemas and database - displayed in output as {"clone": {"from": ... }}
CREATE TABLE .. CLUSTER BY ..
CONSTRAINT .. [NOT] ENFORCED (value stored in 'primary_key_enforced')


#. in CREATE DATABASE properties that goes after name like key=value now parsed valid. Check examples in tests
#. Added support for varchar COLLATE column property

**v0.18.0**
**Features**


#. Added base support fot AWS Redshift SQL dialect. 
   Added support for ENCODE property in column.
   Added new --output-mode='redshift' that add to column 'encrypt' property by default.
   Also add table properties: distkeys, sortkey, diststyle, encode (table level encode), temp.

Supported Redshift statements: SORTKEY, DISTSTYLE, DISTKEY, ENCODE

CREATE TEMP / TEMPORARY TABLE

syntax like with LIKE statement:

create temp table tempevent(like event); 

**v0.17.0**


#. All dependencies were updated for the latest version.
#. Added base support for CREATE [BIGFILE | SMALLFILE] [TEMPORARY] TABLESPACE 
#. Added support for create table properties like ``TABLESPACE user_data ENABLE STORAGE IN ROW CHUNK 8K RETENTION CACHE``
#. Added support for CREATE DATABASE statement

**v0.16.3**


#. Fixed issue then using columns names equals some tokens like, for example, ``key`` caused the error. 
   But still words 'foreign' and 'constraint' as column names cause the empty result. I hope they rarely used.
   Will be fixed in next releases.

**v0.16.2**


#. Fixed issue with enum in lowercase

**v0.16.0**


#. Fixed the issue when NULL column after DEFAULT used as default value.
#. Added support for generated columns, statatements: AS , GENERATED ALWAYS, STORED in Column Defenitions, in output it placed to key 'generated'. Keyword 'generated' showed only if column is generated.
#. Half of changelogs moved to ARCHIVE_CHANGELOG.txt
#. Added base support for CREATE DOMAIN statement
#. Added base support for CREATE SCHEMA [IF NOT EXISTS] ... [AUTHORIZATION] statement, added new type keyword 'schemas'
