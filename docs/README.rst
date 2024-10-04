
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

Yes, library already has about 9000+ downloads per day  - https://pypistats.org/packages/simple-ddl-parser..

As maintainer, I guarantee that any backward incompatible changes will not be done in patch or minor version. But! Pay attention that sometimes output in keywords can be changed in minor version because of fixing wrong behaviour in past.

Articles with examples
^^^^^^^^^^^^^^^^^^^^^^


#. SQL Diagram (Part 3): SQL-to-ERD with DDL: https://levelup.gitconnected.com/sql-diagram-part-3-sql-to-erd-with-ddl-4c9840ee86c3 

Updates in version 1.x
^^^^^^^^^^^^^^^^^^^^^^

The full list of updates can be found in the Changelog below (at the end of README).

Version 1.0.0 was released due to significant changes in the output structure and a stricter approach regarding the scope of the produced output. Now, you must provide the argument 'output_mode=name_of_your_dialect' if you wish to see arguments/properties specific to a particular dialect

How does it work?
^^^^^^^^^^^^^^^^^

Parser supports: 


* SQL
* HQL (Hive)
* MSSQL dialec
* Oracle dialect
* MySQL dialect
* PostgreSQL dialect
* BigQuery
* Redshift
* Snowflake
* SparkSQL
* IBM DB2 dialect

You can check dialects sections after ``Supported Statements`` section to get more information that statements from dialects already supported by parser. If you need to add more statements or new dialects - feel free to open the issue. 

Feel free to open Issue with DDL sample
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pay attentions that I'm adding functional tests for all supported statement, so if you see that your statement is failed and you didn't see it in the test 99,9% that I did n't have sample with such SQL statement - so feel free to open the issue and I will add support for it. 

**If you need some statement, that not supported by parser yet**\ : please provide DDL example & information about that is it SQL dialect or DB.

Types that are used in your DB does not matter, so parser must also work successfully to any DDL for SQL DB. Parser is NOT case sensitive, it did not expect that all queries will be in upper case or lower case. So you can write statements like this:

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

In some dialects like HQL there is a lot of additional information about table like, fore example, is it external table, STORED AS, location & etc. This property will be always empty in 'classic' SQL DB like PostgreSQL or MySQL and this is the reason, why by default this information are 'hidden'.
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

Possible output_modes: ['redshift', 'spark_sql', 'mysql', 'bigquery', 'mssql', 'databricks', 'sqlite', 'vertics', 'ibm_db2', 'postgres', 'oracle', 'hql', 'snowflake', 'sql']

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

You can provide target path where you want to dump result with argument **-t**\ , **--target**\ :

.. code-block:: bash


       sdp tests/sql/test_two_tables.sql -t dump_results/

Get Output in JSON
^^^^^^^^^^^^^^^^^^

If you want to get output in JSON in stdout you can use argument **json_dump=True** in method **.run()** for this

.. code-block:: python

       from simple_ddl_parser import DDLParser


       parse_results = DDLParser("""create table dev.data_sync_history(
           data_sync_id bigint not null,
           sync_count bigint not null,
       ); """).run(json_dump=True)

       print(parse_results)

Output will be:

.. code-block:: json

   [{"columns": [{"name": "data_sync_id", "type": "bigint", "size": null, "references": null, "unique": false, "nullable": false, "default": null, "check": null}, {"name": "sync_count", "type": "bigint", "size": null, "references": null, "unique": false, "nullable": false, "default": null, "check": null}], "primary_key": [], "alter": {}, "checks": [], "index": [], "partitioned_by": [], "tablespace": null, "schema": "dev", "table_name": "data_sync_history"}]

More details
^^^^^^^^^^^^

``DDLParser(ddl).run()``
.run() method contains several arguments, that impact changing output result. As you can saw upper exists argument ``output_mode`` that allow you to set dialect and get more fields in output relative to chosen dialect, for example 'hql'. Possible output_modes: ['redshift', 'spark_sql', 'mysql', 'bigquery', 'mssql', 'databricks', 'sqlite', 'vertics', 'ibm_db2', 'postgres', 'oracle', 'hql', 'snowflake', 'sql']

Also in .run() method exists argument ``group_by_type`` (by default: False). By default output of parser looks like a List with Dicts where each dict == one entity from ddl (table, sequence, type, etc). And to understand that is current entity you need to check Dict like: if 'table_name' in dict - this is a table, if 'type_name' - this is a type & etc.

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

For example, if in your ddl after table definitions (create table statements) you have ALTER table statements like this:

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

Raise error if DDL cannot be parsed by Parser
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default Parser does not raise the error if some statement cannot be parsed - and just skip & produce empty output.

To change this behavior you can pass 'silent=False' argumen to main parser class, like:

.. code-block::

   DDLParser(.., silent=False)


Normalize names
^^^^^^^^^^^^^^^

Use DDLParser(.., normalize_names=True)flag that change output of parser:
If flag is True (default 'False') then all identifiers will be returned without '[', '"' and other delimiters that used in different SQL dialects to separate custom names from reserved words & statements.
For example, if flag set 'True' and you pass this input: 

CREATE TABLE [dbo].\ `TO_Requests <[Request_ID] [int] IDENTITY(1,1>`_ NOT NULL,
    [user_id] [int]

In output you will have names like 'dbo' and 'TO_Requests', not '[dbo]' and '[TO_Requests]'.

Supported Statements
--------------------


* 
  CREATE [OR REPLACE] TABLE [ IF NOT EXISTS ] + columns definition, columns attributes: column name + type + type size(for example, varchar(255)), UNIQUE, PRIMARY KEY, DEFAULT, CHECK, NULL/NOT NULL, REFERENCES, ON DELETE, ON UPDATE,  NOT DEFERRABLE, DEFERRABLE INITIALLY, GENERATED ALWAYS, STORED, COLLATE

* 
  STATEMENTS: PRIMARY KEY, CHECK, FOREIGN KEY in table definitions (in create table();)

* 
  ALTER TABLE STATEMENTS: ADD CHECK (with CONSTRAINT), ADD FOREIGN KEY (with CONSTRAINT), ADD UNIQUE, ADD DEFAULT FOR, ALTER TABLE ONLY, ALTER TABLE IF EXISTS; ALTER .. PRIMARY KEY; ALTER .. USING INDEX TABLESPACE; ALTER .. ADD; ALTER .. MODIFY; ALTER .. ALTER COLUMN; etc

* 
  PARTITION BY statement

* 
  CREATE SEQUENCE with words: INCREMENT [BY], START [WITH], MINVALUE, MAXVALUE, CACHE

* 
  CREATE TYPE statement:  AS TABLE, AS ENUM, AS OBJECT, INTERNALLENGTH, INPUT, OUTPUT

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

SparkSQL Dialect statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^


* USING

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
* CLUSTERED BY 

MySQL
^^^^^


* ON UPDATE in column without reference 

MSSQL
~~~~~


* CONSTRAINT [CLUSTERED]... PRIMARY KEY
* CONSTRAINT ... WITH statement
* PERIOD FOR SYSTEM_TIME in CREATE TABLE statement
* ON [PRIMARY] after CREATE TABLE statement (sample in test files test_mssql_specific.py)
* WITH statement for TABLE properties
* TEXTIMAGE_ON statement
* DEFAULT NEXT VALUE FOR in COLUMN DEFAULT

MSSQL / MySQL/ Oracle
^^^^^^^^^^^^^^^^^^^^^


* type IDENTITY statement
* FOREIGN KEY REFERENCES statement
* 'max' specifier in column size
* CONSTRAINT ... UNIQUE, CONSTRAINT ... CHECK, CONSTRAINT ... FOREIGN KEY, CONSTRAINT ... PRIMARY KEY
* CREATE CLUSTERED INDEX
* CREATE TABLE (...) ORGANIZATION INDEX 

Oracle
^^^^^^


* ENCRYPT column property [+ NO SALT, SALT, USING]
* STORAGE column property

PotgreSQL
^^^^^^^^^


* INHERITS table statement - https://postgrespro.ru/docs/postgresql/14/ddl-inherit 

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
* CREATE TABLE [or REPLACE] [ TRANSIENT | TEMPORARY ] .. CLUSTER BY ..
* CONSTRAINT .. [NOT] ENFORCED 
* COMMENT = in CREATE TABLE & CREATE SCHEMA statements
* WITH MASKING POLICY
* WITH TAG, including multiple tags in the same statement.
* DATA_RETENTION_TIME_IN_DAYS
* MAX_DATA_EXTENSION_TIME_IN_DAYS
* CHANGE_TRACKING

BigQuery
^^^^^^^^


* OPTION in CREATE SCHEMA statement
* OPTION in CREATE TABLE statement
* OPTION in column definition statement

Parser settings
^^^^^^^^^^^^^^^

Logging
~~~~~~~


#. Logging to file

To get logging output to file you should provide to Parser 'log_file' argument with path or file name:

.. code-block:: console


       DDLParser(ddl, log_file='parser221.log').run(group_by_type=True)


#. Logging level

To set logging level you should provide argument 'log_level'

.. code-block:: console


       DDLParser(ddl, log_level=logging.INFO).run(group_by_type=True)

Thanks for involving & contributions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most biggest 'Thanks' ever goes for contributions in parser:
https://github.com/dmaresma
https://github.com/cfhowes
https://github.com/swiatek25
https://github.com/slurpyb
https://github.com/PBalsdon

Big thanks for the involving & contribution with test cases with DDL samples & opening issues goes to:


* https://github.com/kukigai , 
* https://github.com/kliushnichenko ,
* https://github.com/geob3d

for help with debugging & testing support for BigQuery dialect DDLs:


* https://github.com/ankitdata ,
* https://github.com/kalyan939

Changelog
---------

**v1.7.1**

Fixes:
^^^^^^


#. Fix 'character set' issue - https://github.com/xnuinside/simple-ddl-parser/issues/288 

**v1.7.0**

Fixes
^^^^^


#. DEFAULT Value with '::' cast parsed correctly now - https://github.com/xnuinside/simple-ddl-parser/issues/286

Improvements
^^^^^^^^^^^^


#. Added support for ENUM & SET column type - https://github.com/xnuinside/simple-ddl-parser/issues/259 

**v1.6.1**

Fixes
^^^^^


#. #289 CREATE SCHEMA IF NOT EXISTS plus comment fail
#. schema or db.schema location in snowflake

**v1.6.0**

IMPORTANT:
^^^^^^^^^^

In this versions there is some output changes & fixes that can break your code.


#. 
   Now all arguments inside brackets are parsed as separate strings in the list.
   For example:
   ``file_format = (TYPE=JSON NULL_IF=('field')`` this was parsed like 'NULL_IF': "('field')",
   now it will be: 'NULL_IF': ["'field'"],

#. 
   Added separate tokens for EQ ``=`` and IN (previously they was parsed as IDs also - for internal info, for contributors.

#. 
   Some check statements in columns now parsed validly, also IN statements parsed as normal lists.
   So this statement include_exclude_ind CHAR(1) NOT NULL CONSTRAINT chk_metalistcombo_logicalopr
   CHECK (include_exclude_ind IN ('I', 'E')),

will produce this output:

{'check': {'constraint_name': 'chk_metalistcombo_logicalopr',
                         'statement': {'in_statement': {'in': ["'I'", "'E'"],
                                                        'name': 'include_exclude_ind'}}},

Fixes
^^^^^


#. DEFAULT word now is not arriving in key 'default' (it was before in some cases)

New Features
^^^^^^^^^^^^


#. Added Athena output mode and initial support - https://github.com/datacontract/datacontract-cli/issues/332

**v1.5.4**

Improvements
^^^^^^^^^^^^

Snowflake :
~~~~~~~~~~~


#. In Snowflake add ``pattern`` token for external table statement, and improve location rendering

**v1.5.3**

Fixes
^^^^^


#. In Snowflake unexpected error when STRIP_OUTER_ARRAY property in file_format statement - https://github.com/xnuinside/simple-ddl-parser/issues/276
   2.

**v1.5.2**

Improvements
^^^^^^^^^^^^

MySQL
~~~~~


#. Added support for COLLATE - https://github.com/xnuinside/simple-ddl-parser/pull/266/files

Fixes
^^^^^


#. In Snowflake Fix unexpected behaviour when file_format name given - https://github.com/xnuinside/simple-ddl-parser/issues/273

**v1.5.1**

Improvements
^^^^^^^^^^^^

MySQL
~~~~~


#. Added support for INDEX statement in column definition - https://github.com/xnuinside/simple-ddl-parser/issues/253
   2.

**v1.5.0**

Fixes
^^^^^


#. Now, ``unique`` set up to column only if it was only one column in unique constraint/index. Issue - https://github.com/xnuinside/simple-ddl-parser/issues/255
#. Fixed issue when UNIQUE KEY was identified as primary key - https://github.com/xnuinside/simple-ddl-parser/issues/253

**v1.4.0**

Fixes
^^^^^

BigQuery:
~~~~~~~~~


#. Indexes without schema causes issues in BigQuery dialect - fixed.

Improvements
^^^^^^^^^^^^

Oracle:
~~~~~~~


#. Added new output keywords in table definition - ``temp`` & ``is_global``. Added support for create global temporary table - https://github.com/xnuinside/simple-ddl-parser/issues/182

**v1.3.0**

Fixes
^^^^^

PostgreSQL:


#. Timezone was moved out from type definition to keyword 'with_time_zone' it can be True (if with time zone) or False (if without)
   BigQuery:
#. Previously Range in RANGE_BUCKETS was parsed as a columns, now this behaviour is changed and
   range placed in own keyword - 'range' (can be array or str).
   Also for all ```*_TRUNC PARTITIONS`` like DATETIME_TRUNC, TIMESTAMP_TRUNC, etc, second argument moved to arg 'trunc_by'

Improvements
^^^^^^^^^^^^

PostgreSQL:


#. Added support for PostgreSQL with / without time zone - https://github.com/xnuinside/simple-ddl-parser/issues/250

BigQuery:


#. Added support for GENERATE_ARRAY in RANGE_BUCKETS https://github.com/xnuinside/simple-ddl-parser/issues/183

**v1.2.1**

Fixes
^^^^^

MySQL:


#. Fixed issue relative to auto_increment that caused empty output if auto_increment defined in table properties -
   https://github.com/xnuinside/simple-ddl-parser/issues/206

Improvements
^^^^^^^^^^^^

MySQL:


#. auto_increment added as property to mysql output

Oracle:


#. Added support for  constraint name in column definition - https://github.com/xnuinside/simple-ddl-parser/issues/203
#. Added support for GENERATED (ALWAYS | (BY DEFAULT [ON NULL])) AS IDENTITY in column definition

PostgreSQL:


#. Added support for CAST statement in column GENERATE ALWAYS expression - https://github.com/xnuinside/simple-ddl-parser/issues/198

**v1.1.0**

Improvements
^^^^^^^^^^^^

MySQL:


#. Added support for INDEX statement inside table definition
#. Added support for MySQL INVISIBLE/VISIBLE statement - https://github.com/xnuinside/simple-ddl-parser/issues/243

Snowflake:


#. Added support for cluster by statement before columns definition - https://github.com/xnuinside/simple-ddl-parser/issues/234

**v1.0.4**

Improvements
^^^^^^^^^^^^


#. Support functions with schema prefix in ``DEFAULT`` and ``CHECK`` statements. https://github.com/xnuinside/simple-ddl-parser/issues/240
   ### Fixes
#. Fix for REFERENCES NOT NULL - https://github.com/xnuinside/simple-ddl-parser/issues/239
#. Fix for snowflake stage name location format bug fix - https://github.com/xnuinside/simple-ddl-parser/pull/241

**v1.0.3**

Improvements
^^^^^^^^^^^^


#. Fixed bug with ``CREATE OR REPLACE SCHEMA``.
#. Added support of create empty tables without columns CREATE TABLE tablename (); (valid syntax in SQL)

Snowflake
^^^^^^^^^


#. Fixed bug with snowflake ``stage_`` fileformat option value equal a single string as ``FIELD_OPTIONALLY_ENCLOSED_BY = '\"'``\ , ``FIELD_DELIMITER = '|'``
#. improve snowflake fileformat key equals value into dict. type.

**v1.0.2**

Improvements
^^^^^^^^^^^^


#. Fixed bug with places first table property value in 'authorization' key. Now it is used real property name.
#. Fixed typo on Databricks dialect
#. improved equals symbols support within COMMENT statement.
#. turn regexp into functions

MySQL Improvements
^^^^^^^^^^^^^^^^^^


#. UNSIGNED property after int parsed validly now

Snowflake
^^^^^^^^^


#. Snowflake TAG now available on SCHEMA definitions.

**v1.0.1**

Minor Fixes
^^^^^^^^^^^


#. When using ``normalize_names=True`` do not remove ``[]`` from types like ``decimal(21)[]``.
#. When using ``normalize_names=True`` ensure that ``"complex"."type"`` style names convert to ``complex.type``.

**v1.0.0**
In output structure was done important changes that can in theory breaks code.

Important changes
^^^^^^^^^^^^^^^^^


#. Important change: 

all custom table properties that are defined after column definition in 'CREATE TABLE' statement and relative to only one dialect (only for SparkSQL, or HQL,etc), for example, like here:
https://github.com/xnuinside/simple-ddl-parser/blob/main/tests/dialects/test_snowflake.py#L767  or https://github.com/xnuinside/simple-ddl-parser/blob/main/tests/dialects/test_spark_sql.py#L133 will be saved now in property ``table_properties`` as dict.
Previously they was placed on same level of table output as ``columns``\ , ``alter``\ , etc. Now, they grouped and moved to key ``table_properties``.


#. 
   Formatting parser result now represented by 2 classes - Output & TableData, that makes it more strict and readable.

#. 
   The output mode now functions more strictly. If you want to obtain output fields specific to a certain dialect, 
   use output_mode='snowflake' for Snowflake or output_mode='hql' for HQL, etc. 
   Previously, some keys appeared in the result without being filtered by dialect. 
   For example, if 'CLUSTER BY' was in the DDL, it would show up in the 'cluster_by' field regardless of the output mode. 
   However, now all fields that only work in certain dialects and are not part of the basic SQL notation will only be shown 
   if you choose the correct output_mode.

New Dialects support
^^^^^^^^^^^^^^^^^^^^


#. Added as possible output_modes new Dialects: 


* Databricks SQL like 'databricks', 
* Vertica as 'vertica', 
* SqliteFields as 'sqlite',
* PostgreSQL as 'postgres'

Full list of supported dialects you can find in dict - ``supported_dialects``\ :

``from simple_ddl_parser import supported_dialects``

Currently supported: ['redshift', 'spark_sql', 'mysql', 'bigquery', 'mssql', 'databricks', 'sqlite', 'vertics', 'ibm_db2', 'postgres', 'oracle', 'hql', 'snowflake', 'sql']

If you don't see dialect that you want to use - open issue with description and links to Database docs or use one of existed dialects.

Snowflake updates:
^^^^^^^^^^^^^^^^^^


#. For some reasons, 'CLONE' statement in SNOWFLAKE was parsed into 'like' key in output. Now it was changed to 'clone' - inner structure of output stay the same as previously.

MySQL updates:
^^^^^^^^^^^^^^


#. Engine statement now parsed correctly. Previously, output was always '='.

BigQuery updates:
^^^^^^^^^^^^^^^^^


#. Word 'schema' totally removed from output. ``Dataset`` used instead of ``schema`` in BigQuery dialect.

**v0.32.1**

Minor Fixes
^^^^^^^^^^^


#. Removed debug print

**v0.32.0**

Improvements
^^^^^^^^^^^^


#. Added support for several ALTER statements (ADD, DROP, RENAME, etc) - https://github.com/xnuinside/simple-ddl-parser/issues/215
   In 'alter' output added several keys:

   #. 'dropped_columns' - to store information about columns that was in table, but after dropped by alter
   #. 'renamed_columns' - to store information about columns that was renamed
   #. 'modified_columns' - to track alter column changes for defaults, datetype, etc. Argument stores previous columns states.

Fixes
^^^^^


#. Include source column names in FOREIGN KEY references. Fix for: https://github.com/xnuinside/simple-ddl-parser/issues/196
#. ALTER statement now will be parsed correctly if names & schemas written differently in ``create table`` statement and alter. 
   For example, if in create table you use quotes like "schema_name"."table_name", but in alter was schema_name.table_name - previously it didn't work, but now parser understand that it is the same table.

**v0.31.3**

Improvements
^^^^^^^^^^^^

Snowflake update:
~~~~~~~~~~~~~~~~~


#. Added support for Snowflake Virtual Column definition in External Column  ``AS ()`` statement - https://github.com/xnuinside/simple-ddl-parser/issues/218
#. enforce support for Snowflake _FILE_FORMAT options in External Column ddl statement - https://github.com/xnuinside/simple-ddl-parser/issues/221

Others
~~~~~~


#. Support for KEY statement in CREATE TABLE statements. KEY statements will now create INDEX entries in the DDL parser.

**v0.31.2**

Improvements
^^^^^^^^^^^^

Snowflake update:
~~~~~~~~~~~~~~~~~


#. Added support for Snowflake AUTOINCREMENT | IDENTITY column definitions with optional parameter ``ORDER|NOORDER`` statement - https://github.com/xnuinside/simple-ddl-parser/issues/213

Common
~~~~~~


#. Added param 'encoding' to parse_from_file function - https://github.com/xnuinside/simple-ddl-parser/issues/142.
   Default encoding is utf-8.

**v0.31.1**

Improvements
^^^^^^^^^^^^

Snowflake update:
~~~~~~~~~~~~~~~~~


#. Support multiple tag definitions in a single ``WITH TAG`` statement.
#. Added support for Snowflake double single quotes - https://github.com/xnuinside/simple-ddl-parser/issues/208

**v0.31.0**

Fixes:
^^^^^^


#. Move inline flag in regexp (issue with python 3.11) - https://github.com/xnuinside/simple-ddl-parser/pull/200
   Fix for: https://github.com/xnuinside/simple-ddl-parser/issues/199

Improvements:
^^^^^^^^^^^^^


#. Added ``Snowflake Table DDL support of WITH MASKING POLICY column definition`` - https://github.com/xnuinside/simple-ddl-parser/issues/201

Updates:
^^^^^^^^


#. All deps updated to the latest versions.

**v0.30.0**

Fixes:
^^^^^^


#. IDENTITY now parsed normally as a separate column property. Issue: https://github.com/xnuinside/simple-ddl-parser/issues/184

New Features:
^^^^^^^^^^^^^


#. 
   IN TABLESPACE IBM DB2 statement now is parsed into 'tablespace' key. Issue: https://github.com/xnuinside/simple-ddl-parser/issues/194.
   INDEX IN also parsed to 'index_in' key.
   Added support for ORGANIZE BY statement

#. 
   Added support for PostgreSQL INHERITS statement. Issue: https://github.com/xnuinside/simple-ddl-parser/issues/191

**v0.29.1**

Important updates:
^^^^^^^^^^^^^^^^^^


#. Python 3.6 is deprecated in tests and by default, try to move to Python3.7, but better to 3.8, because 3.7 will be deprecated in 2023.

Fixes
^^^^^


#. Fix for https://github.com/xnuinside/simple-ddl-parser/issues/177

Improvements
^^^^^^^^^^^^


#. Added support for Oracle 2 component size for types, like '30 CHAR'. From https://github.com/xnuinside/simple-ddl-parser/issues/176

**v0.29.0**

Fixes
^^^^^


#. AUTOINCREMENT statement now parsed validly same way as AUTO_INCREMENT and showed up in output as 'autoincrement' property of the column
   Fix for: https://github.com/xnuinside/simple-ddl-parser/issues/170
#. Fix issue ' TypeError argument of type 'NoneType' is not iterable' on some foreigen keys https://github.com/xnuinside/simple-ddl-parser/issues/148

New Features
^^^^^^^^^^^^


#. Support for non-numeric column type parameters https://github.com/xnuinside/simple-ddl-parser/issues/171
   It shows in column attribute 'type_parameters'.

**v0.28.1**
Improvements:


#. Lines started with INSERT INTO statement now successfully ignored by parser (so you can keep them in ddl - they will be just skipped)

Fixes:


#. Important fix for multiline comments

**v0.28.0**

Important Changes (Pay attention):


#. Because of parsing now AUTO_INCREMENT as a separate property of column previous output changed.
   Previously it was parsed as a part of type like:  'INT AUTO_INCREMENT'.
   Now type will be only 'INT', but in column property you will see 'autoincrement': True.

Amazing innovation:


#. It's is weird to write in Changelog, but only in version 0.28.0 I recognize that floats that not supported by parser & it was fixed.
   Thanks for the sample in the issue: https://github.com/xnuinside/simple-ddl-parser/issues/163

Improvements:
MariaDB:


#. Added support for MariaDB AUTO_INCREMENT (from ddl here - https://github.com/xnuinside/simple-ddl-parser/issues/144)
   If column is Auto Incremented - it indicated as 'autoincrement': True in column definition

Common:


#. Added parsing for multiline comments in DDL with ``/* */`` syntax.
#. Comments from DDL now all placed in 'comments' keyword if you use ``group_by_type=`` arg in parser.
#. Added argument 'parser_settings={}' (dict type) in method  parse_from_file() - this way you can pass any arguments that you want to DDLParser (& that supported by it)
   So, if you want to set log_level=logging.WARNING for parser - just use it as:
   parse_from_file('path_to_file', parser_settings={'log_level': logging.WARNING}). For issue: https://github.com/xnuinside/simple-ddl-parser/issues/160

**v0.27.0**

Fixes:


#. Fixed parsing CHECKS with IN statement - https://github.com/xnuinside/simple-ddl-parser/issues/150
#. @# symbols added to ID token - (partially) https://github.com/xnuinside/simple-ddl-parser/issues/146

Improvements:


#. Added support for '*' in size column (ORACLE dialect) - https://github.com/xnuinside/simple-ddl-parser/issues/151
#. Added arg 'debug' to parser, works same way as 'silent' - to get more clear error output.

New features:


#. Added support for ORACLE 'ORGANIZATION INDEX'
#. Added support for SparkSQL Partition by with procedure call - https://github.com/xnuinside/simple-ddl-parser/issues/154
#. Added support for DEFAULT CHARSET statement MySQL - https://github.com/xnuinside/simple-ddl-parser/issues/153

**v0.26.5**

Fixes:


#. Parsetab included in builds.
#. Added additional argumen log_file='path_to_file', to enable logging to file with providen name.

**v0.26.4**


#. Bugfix for (support CREATE OR REPLACE with additional keys like transient/temporary): https://github.com/xnuinside/simple-ddl-parser/issues/133

**v0.26.3**

Improvements:


#. Added support for OR REPLACE in CREATE TABLE: https://github.com/xnuinside/simple-ddl-parser/issues/131
#. Added support for AUTO INCREMENT in column:https://github.com/xnuinside/simple-ddl-parser/issues/130

**v0.26.2**

Fixes:


#. Fixed a huge bug for incorrect parsing lines with 'USE' & 'GO' strings inside.
#. Fixed parsing for CREATE SCHEMA for Snowlake & Oracle DDLs

Improvements:


#. Added  COMMENT statement for CREATE TABLE ddl (for SNOWFLAKE dialect support)
#. Added  COMMENT statement for CREATE SCHEMA ddl (for SNOWFLAKE dialect support)

**v0.26.1**

Fixes:


#. support Multiple SERDEPROPERTIES  - https://github.com/xnuinside/simple-ddl-parser/issues/126
#. Fix for issue with LOCATION and TBLPROPERTIES clauses in CREATE TABLE LIKE - https://github.com/xnuinside/simple-ddl-parser/issues/125
#. LOCATION now works correctly with double quote strings

**v0.26.0**
Improvements:


#. Added more explicit debug message on Statement errors - https://github.com/xnuinside/simple-ddl-parser/issues/116
#. Added support for "USING INDEX TABLESPACE" statement in ALTER - https://github.com/xnuinside/simple-ddl-parser/issues/119
#. Added support for IN statements in CHECKS - https://github.com/xnuinside/simple-ddl-parser/issues/121

New features:


#. Support SparkSQL USING - https://github.com/xnuinside/simple-ddl-parser/issues/117
   Updates initiated by ticket https://github.com/xnuinside/simple-ddl-parser/issues/120:
#. In Parser you can use argument json_dump=True in method .run() if you want get result in JSON format.


* README updated

Fixes:


#. Added support for PARTITION BY one column without type
#. Alter table add constraint PRIMARY KEY - https://github.com/xnuinside/simple-ddl-parser/issues/119
#. Fix for paring SET statement - https://github.com/xnuinside/simple-ddl-parser/pull/122
#. Fix for disappeared columns without properties - https://github.com/xnuinside/simple-ddl-parser/issues/123

**v0.25.0**

Fixes:
------


#. Fix for issue with 'at time zone' https://github.com/xnuinside/simple-ddl-parser/issues/112

New features:
-------------


#. Added flag to raise errors if parser cannot parse statement DDLParser(.., silent=False) - https://github.com/xnuinside/simple-ddl-parser/issues/109
#. Added flag to DDLParser(.., normalize_names=True) that change output of parser:
   if flag is True (default 'False') then all identifiers will be returned without '[', '"' and other delimiters that used in different SQL dialects to separate custom names from reserved words & statements.
   For example, if flag set 'True' and you pass this input:

CREATE TABLE [dbo].\ `TO_Requests <[Request_ID] [int] IDENTITY(1,1>`_ NOT NULL,
    [user_id] [int]

In output you will have names like 'dbo' and 'TO_Requests', not '[dbo]' and '[TO_Requests]'.

**v0.24.2**

Fixes:
------


#. Fix for the issue: https://github.com/xnuinside/simple-ddl-parser/issues/108 (reserved words can be used as table name after '.')

**v0.24.1**

Fixes:
------

HQL:
^^^^


#. fields_terminated_by now parses , as "','", not as '' previously

Common:
^^^^^^^


#. To output added 'if_not_exists' field in result to get availability 1-to-1 re-create ddl by metadata.

**v0.24.0**

Fixes:
------

HQL:
^^^^


#. More then 2 tblproperties now are parsed correctly https://github.com/xnuinside/simple-ddl-parser/pull/104

Common:
^^^^^^^


#. 'set' in lower case now also parsed validly.
#. Now names like 'schema', 'database', 'table' can be used as names in CREATE DATABASE | SCHEMA | TABLESPACE | DOMAIN | TYPE statements and after INDEX and CONSTRAINT.
#. Creation of empty tables also parsed correctly (like CREATE Table table;).

New Statements Support:
-----------------------

HQL:
^^^^


#. Added support for CLUSTERED BY - https://github.com/xnuinside/simple-ddl-parser/issues/103
#. Added support for  INTO ... BUCKETS
#. CREATE REMOTE DATABASE | SCHEMA

**v0.23.0**

Big refactoring: less code complexity & increase code coverage. Radon added to pre-commit hooks.

Fixes:
^^^^^^


#. Fix for issue with ALTER UNIQUE - https://github.com/xnuinside/simple-ddl-parser/issues/101

New Features
^^^^^^^^^^^^


#. SQL Comments string from DDL now parsed to "comments" key in output.

PostgreSQL:


#. Added support for ALTER TABLE ONLY | ALTER TABLE IF EXISTS

**v0.22.5**

Fixes:
^^^^^^


#. Fix for issue with '<' - https://github.com/xnuinside/simple-ddl-parser/issues/89

**v0.22.4**

Fixes:
^^^^^^

BigQuery:
^^^^^^^^^


#. Fixed issue with parsing schemas with project in name.
#. Added support for multiple OPTION() statements

**v0.22.3**

Fixes:
^^^^^^

BigQuery:
^^^^^^^^^


#. CREATE TABLE statement with 'project_id' in format like project.dataset.table_name now is parsed validly.
   'project' added to output.
   Also added support project.dataset.name format in CREATE SCHEMA and ALTER statement

**v0.22.2**

Fixes:
^^^^^^


#. Fix for the issue: https://github.com/xnuinside/simple-ddl-parser/issues/94 (column name starts with CREATE)

**v0.22.1**

New Features:
^^^^^^^^^^^^^

BigQuery:
---------


#. Added support for OPTION for full CREATE TABLE statement & column definition

Improvements:
-------------


#. CLUSTED BY can be used without ()
