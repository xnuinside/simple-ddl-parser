## Simple DDL Parser

[![PyPI](https://img.shields.io/pypi/v/simple-ddl-parser)](https://pypi.org/project/simple-ddl-parser/) [![License](https://img.shields.io/pypi/l/simple-ddl-parser)](https://pypi.org/project/simple-ddl-parser/) [![Python Versions](https://img.shields.io/pypi/pyversions/simple-ddl-parser)](https://pypi.org/project/simple-ddl-parser/) [![Workflow](https://github.com/xnuinside/simple-ddl-parser/actions/workflows/ci-tests-runner.yml/badge.svg)](https://github.com/xnuinside/simple-ddl-parser/actions/workflows/ci-tests-runner.yml)

Build with ply (lex & yacc in python). A lot of samples in 'tests/.

### Is it Stable?

Yes, library already has about 9000+ downloads per day  - https://pypistats.org/packages/simple-ddl-parser..

As maintainer, I guarantee that any backward incompatible changes will not be done in patch or minor version. But! Pay attention that sometimes output in keywords can be changed in minor version because of fixing wrong behaviour in past.

### Articles with examples

1. SQL Diagram (Part 3): SQL-to-ERD with DDL: https://levelup.gitconnected.com/sql-diagram-part-3-sql-to-erd-with-ddl-4c9840ee86c3 

### Updates in version 1.x

The full list of updates can be found in the Changelog below (at the end of README).

Version 1.0.0 was released due to significant changes in the output structure and a stricter approach regarding the scope of the produced output. Now, you must provide the argument 'output_mode=name_of_your_dialect' if you wish to see arguments/properties specific to a particular dialect


### How does it work?

Parser supports: 

- SQL
- HQL (Hive)
- MSSQL dialect
- Oracle dialect
- MySQL dialect
- PostgreSQL dialect
- BigQuery
- Redshift
- Snowflake
- SparkSQL
- IBM DB2 dialect
- Informix/GBase 8s dialect

You can check dialects sections after `Supported Statements` section to get more information that statements from dialects already supported by parser. If you need to add more statements or new dialects - feel free to open the issue. 


### Feel free to open Issue with DDL sample
Pay attentions that I'm adding functional tests for all supported statement, so if you see that your statement is failed and you didn't see it in the test 99,9% that I did n't have sample with such SQL statement - so feel free to open the issue and I will add support for it. 

**If you need some statement, that not supported by parser yet**: please provide DDL example & information about that is it SQL dialect or DB.

Types that are used in your DB does not matter, so parser must also work successfully to any DDL for SQL DB. Parser is NOT case sensitive, it did not expect that all queries will be in upper case or lower case. So you can write statements like this:

```sql

    Alter Table Persons ADD CONSTRAINT CHK_PersonAge CHECK (Age>=18 AND City='Sandnes');

```

It will be parsed as is without errors.

If you have samples that cause an error - please open the issue (but don't forget to add ddl example), I will be glad to fix it.

A lot of statements and output result you can find in tests on the github - https://github.com/xnuinside/simple-ddl-parser/tree/main/tests .

### How to install

```bash

    pip install simple-ddl-parser

```

## How to use

### Extract additional information from HQL (& other dialects)

In some dialects like HQL there is a lot of additional information about table like, fore example, is it external table, STORED AS, location & etc. This property will be always empty in 'classic' SQL DB like PostgreSQL or MySQL and this is the reason, why by default this information are 'hidden'.
Also some fields hidden in HQL, because they are simple not exists in HIVE, for example 'deferrable_initially'
To get this 'hql' specific details about table in output please use 'output_mode' argument in run() method.

example:

```python

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
```

And you will get output with additional keys 'stored_as', 'location', 'external', etc.

```python

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

```

If you run parser with command line add flag '-o=hql' or '--output-mode=hql' to get the same result.

Possible output_modes: ['redshift', 'spark_sql', 'mysql', 'bigquery', 'mssql', 'databricks', 'sqlite', 'vertics', 'ibm_db2', 'postgres', 'oracle', 'hql', 'snowflake', 'sql']

### From python code

```python
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

```

### To parse from file

```python
    
    from simple_ddl_parser import parse_from_file

    result = parse_from_file('tests/sql/test_one_statement.sql')
    print(result)

```

### Examples

There are Python usage examples in `examples/`:
- `examples/basic_usage.py`
- `examples/custom_schema.py`
- `examples/output_modes.py`
- `examples/bigquery_schema.py`

### From command line

simple-ddl-parser is installed to environment as command **sdp**

```bash

    sdp path_to_ddl_file

    # for example:

    sdp tests/sql/test_two_tables.sql
    
```

You will see the output in **schemas** folder in file with name **test_two_tables_schema.json**

If you want to have also output in console - use **-v** flag for verbose.

```bash
    
    sdp tests/sql/test_two_tables.sql -v
    
```

If you don't want to dump schema in file and just print result to the console, use **--no-dump** flag:


```bash
    
    sdp tests/sql/test_two_tables.sql --no-dump
    
```

To reshape output with a custom schema (for example, BigQuery JSON schema), use **--custom-output-schema**:

```bash
    sdp tests/sql/test_two_tables.sql --no-dump --custom-output-schema bigquery
```

You can provide target path where you want to dump result with argument **-t**, **--target**:


```bash
    
    sdp tests/sql/test_two_tables.sql -t dump_results/
    
```
### Get Output in JSON

If you want to get output in JSON in stdout you can use argument **json_dump=True** in method **.run()** for this
```python
    from simple_ddl_parser import DDLParser


    parse_results = DDLParser("""create table dev.data_sync_history(
        data_sync_id bigint not null,
        sync_count bigint not null,
    ); """).run(json_dump=True)

    print(parse_results) 

```
Output will be:

```json
[{"columns": [{"name": "data_sync_id", "type": "bigint", "size": null, "references": null, "unique": false, "nullable": false, "default": null, "check": null}, {"name": "sync_count", "type": "bigint", "size": null, "references": null, "unique": false, "nullable": false, "default": null, "check": null}], "primary_key": [], "alter": {}, "checks": [], "index": [], "partitioned_by": [], "tablespace": null, "schema": "dev", "table_name": "data_sync_history"}]
```

### More details

`DDLParser(ddl).run()`
.run() method contains several arguments, that impact changing output result. As you can saw upper exists argument `output_mode` that allow you to set dialect and get more fields in output relative to chosen dialect, for example 'hql'. Possible output_modes: ['redshift', 'spark_sql', 'mysql', 'bigquery', 'mssql', 'databricks', 'sqlite', 'vertics', 'ibm_db2', 'postgres', 'oracle', 'hql', 'snowflake', 'sql']

Argument `custom_output_schema` allows you to reshape output into a schema format. Built-in support currently includes `bigquery` (JSON schema array), and you can register your own schema converters.
When `custom_output_schema="bigquery"` is used, the parser will default to `output_mode="bigquery"` for dialect-specific syntax.

```python
from simple_ddl_parser import DDLParser, register_custom_output_schema

ddl = "CREATE TABLE users (id INT NOT NULL, email VARCHAR(255));"
bigquery_schema = DDLParser(ddl).run(custom_output_schema="bigquery")

def to_custom_schema(output):
    return [{"custom": True, "tables": len(output)}]

register_custom_output_schema("custom", to_custom_schema)
custom_schema = DDLParser(ddl).run(custom_output_schema="custom")
```

Also in .run() method exists argument `group_by_type` (by default: False). By default output of parser looks like a List with Dicts where each dict == one entity from ddl (table, sequence, type, etc). And to understand that is current entity you need to check Dict like: if 'table_name' in dict - this is a table, if 'type_name' - this is a type & etc.

To make work little bit easy you can set group_by_type=True and you will get output already sorted by types, like:

```python

    { 
        'tables': [all_pasrsed_tables], 
        'sequences': [all_pasrsed_sequences], 
        'types': [all_pasrsed_types], 
        'domains': [all_pasrsed_domains],
        ...
    }

```

For example:

```python

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

```

### ALTER statements

Parser supports various ALTER TABLE statements:

- **ADD COLUMN** - with or without COLUMN keyword
- **DROP COLUMN** - with or without COLUMN keyword (Oracle style)
- **MODIFY COLUMN** - with or without COLUMN keyword (Oracle style)
- **ALTER COLUMN** - SQL Server style
- **RENAME COLUMN**
- **ADD FOREIGN KEY** - with or without CONSTRAINT
- **ADD PRIMARY KEY** - with or without CONSTRAINT
- **ADD UNIQUE**
- **ADD CHECK**
- **ADD DEFAULT**

Multiple operations can be combined in a single ALTER statement using commas:

```sql
ALTER TABLE my_table ADD col1 int, ADD col2 varchar;
ALTER TABLE my_table DROP COLUMN col1, DROP COLUMN col2;
ALTER TABLE my_table MODIFY COLUMN col1 int, MODIFY COLUMN col2 varchar;
```

All ALTER statements are parsed and information is stored in the 'alter' key of the table's dict.
For example, please check alter statement tests - **tests/test_alter_statements.py**


### More examples & tests

You can find in **tests/** folder.

### Dump result in json

To dump result in json use argument .run(dump=True)

You also can provide a path where you want to have a dumps with schema with argument .run(dump_path='folder_that_use_for_dumps/')


### Raise error if DDL cannot be parsed by Parser

By default Parser does not raise the error if some statement cannot be parsed - and just skip & produce empty output.

To change this behavior you can pass 'silent=False' argumen to main parser class, like:

    DDLParser(.., silent=False)

### Normalize names

Use DDLParser(.., normalize_names=True)flag that change output of parser:
If flag is True (default 'False') then all identifiers will be returned without '[', '"' and other delimiters that used in different SQL dialects to separate custom names from reserved words & statements.
For example, if flag set 'True' and you pass this input: 

CREATE TABLE [dbo].[TO_Requests](
    [Request_ID] [int] IDENTITY(1,1) NOT NULL,
    [user_id] [int]

In output you will have names like 'dbo' and 'TO_Requests', not '[dbo]' and '[TO_Requests]'.


## Supported Statements

- CREATE [OR REPLACE] TABLE [ IF NOT EXISTS ] + columns definition, columns attributes: column name + type + type size(for example, varchar(255)), UNIQUE, PRIMARY KEY, DEFAULT, CHECK, NULL/NOT NULL, REFERENCES, ON DELETE, ON UPDATE,  NOT DEFERRABLE, DEFERRABLE INITIALLY, GENERATED ALWAYS, STORED, COLLATE

- STATEMENTS: PRIMARY KEY, CHECK, FOREIGN KEY in table definitions (in create table();)

- ALTER TABLE STATEMENTS: ADD CHECK (with CONSTRAINT), ADD FOREIGN KEY (with CONSTRAINT), ADD UNIQUE, ADD DEFAULT FOR, ALTER TABLE ONLY, ALTER TABLE IF EXISTS; ALTER .. PRIMARY KEY; ALTER .. USING INDEX TABLESPACE; ALTER .. ADD; ALTER .. MODIFY; ALTER .. ALTER COLUMN; etc

- PARTITION BY statement

- CREATE SEQUENCE with words: INCREMENT [BY], START [WITH], MINVALUE, MAXVALUE, CACHE

- CREATE TYPE statement:  AS TABLE, AS ENUM, AS OBJECT, INTERNALLENGTH, INPUT, OUTPUT

- LIKE statement (in this and only in this case to output will be added 'like' keyword with information about table from that we did like - 'like': {'schema': None, 'table_name': 'Old_Users'}).

- TABLESPACE statement

- COMMENT ON statement

- CREATE SCHEMA [IF NOT EXISTS] ... [AUTHORIZATION] ...

- CREATE DOMAIN [AS]

- CREATE [SMALLFILE | BIGFILE] [TEMPORARY] TABLESPACE statement

- CREATE DATABASE + Properties parsing

### SparkSQL Dialect statements

- USING


### HQL Dialect statements

- PARTITIONED BY statement
- ROW FORMAT, ROW FORMAT SERDE
- WITH SERDEPROPERTIES ("input.regex" =  "..some regex..")
- STORED AS (AVRO, PARQUET, etc), STORED AS INPUTFORMAT, OUTPUTFORMAT
- COMMENT
- LOCATION
- FIELDS TERMINATED BY, LINES TERMINATED BY, COLLECTION ITEMS TERMINATED BY, MAP KEYS TERMINATED BY
- TBLPROPERTIES ('parquet.compression'='SNAPPY' & etc.)
- SKEWED BY
- CLUSTERED BY 

### MySQL

- ON UPDATE in column without reference 

#### MSSQL 

- CONSTRAINT [CLUSTERED]... PRIMARY KEY
- CONSTRAINT ... WITH statement
- PERIOD FOR SYSTEM_TIME in CREATE TABLE statement
- ON [PRIMARY] after CREATE TABLE statement (sample in test files test_mssql_specific.py)
- WITH statement for TABLE properties
- TEXTIMAGE_ON statement
- DEFAULT NEXT VALUE FOR in COLUMN DEFAULT

### MSSQL / MySQL/ Oracle

- type IDENTITY statement
- FOREIGN KEY REFERENCES statement
- 'max' specifier in column size
- CONSTRAINT ... UNIQUE, CONSTRAINT ... CHECK, CONSTRAINT ... FOREIGN KEY, CONSTRAINT ... PRIMARY KEY
- CREATE CLUSTERED INDEX
- CREATE TABLE (...) ORGANIZATION INDEX 

### Oracle

- ENCRYPT column property [+ NO SALT, SALT, USING]
- STORAGE column property


### PotgreSQL

- INHERITS table statement - https://postgrespro.ru/docs/postgresql/14/ddl-inherit 

### AWS Redshift Dialect statements

- ENCODE column property
- SORTKEY, DISTSTYLE, DISTKEY, ENCODE table properties
- CREATE TEMP / TEMPORARY TABLE

- syntax like with LIKE statement:

 `create temp table tempevent(like event);`

### Snowflake Dialect statements

- CREATE .. CLONE statements for table, database and schema
- CREATE TABLE [or REPLACE] [ TRANSIENT | TEMPORARY ] .. CLUSTER BY ..
- CONSTRAINT .. [NOT] ENFORCED 
- COMMENT = in CREATE TABLE & CREATE SCHEMA statements
- WITH MASKING POLICY
- WITH TAG, including multiple tags in the same statement.
- DATA_RETENTION_TIME_IN_DAYS
- MAX_DATA_EXTENSION_TIME_IN_DAYS
- CHANGE_TRACKING

### BigQuery

- OPTION in CREATE SCHEMA statement
- OPTION in CREATE TABLE statement
- OPTION in column definition statement

### Informix/GBase 8s

Informix is an IBM relational database. GBase 8s is a Chinese enterprise database based on Informix with Oracle compatibility features.

Supported Informix-specific features:
- Data types: SERIAL, SERIAL8, BIGSERIAL, INT8, LVARCHAR, TEXT, BYTE, BLOB, CLOB, MONEY, NCHAR, NVARCHAR
- DATETIME with qualifiers: DATETIME YEAR TO SECOND, DATETIME YEAR TO FRACTION(n), DATETIME YEAR TO DAY
- INTERVAL types: INTERVAL HOUR TO MINUTE, INTERVAL DAY TO DAY, INTERVAL YEAR TO MONTH
- DEFAULT with Informix functions: TODAY, CURRENT, USER
- Storage options: IN dbspace, EXTENT SIZE, NEXT SIZE, LOCK MODE

GBase 8s Oracle-compatible features:
- Data types: VARCHAR2, NVARCHAR2, NUMBER
- Virtual columns: `column_name AS (expression)`, `GENERATED ALWAYS AS (expression)`

### Parser settings


#### Logging

1. Logging to file

To get logging output to file you should provide to Parser 'log_file' argument with path or file name:

```console

    DDLParser(ddl, log_file='parser221.log').run(group_by_type=True)

```

2. Logging level

To set logging level you should provide argument 'log_level'

```console

    DDLParser(ddl, log_level=logging.INFO).run(group_by_type=True)

```

### Thanks for involving & contributions

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

## Changelog
# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog 1.0.0, and this project adheres to Semantic Versioning.

## [Unreleased]
### Added
- None.

### Changed
- Switched the canonical changelog to CHANGELOG.md.

## [1.10.0] - 2026-01-27
### Added
- Custom output schema support with built-in BigQuery schema conversion and a custom schema registry.
- Tests for BigQuery custom schema output and user-registered schemas.
- CLI support for `custom_output_schema`.
- Examples directory with Python usage samples, documented in the README.
- Tox as a development dependency.
- BigQuery custom schema usage example.

### Changed
- Custom output schema `bigquery` now defaults the output mode to BigQuery for dialect-specific parsing.
- Tox linting configuration now skips the local `.venv` directory.

### Fixed
- MySQL parsing for `FOREIGN KEY` constraints with `ON DELETE SET NULL` in multi-table DDL. https://github.com/xnuinside/simple-ddl-parser/issues/290
- Preserved column-level reference handling for SET actions while supporting SET NULL constraints.

## [1.9.0] - 2026-01-24
### Added
- Tests for MySQL COMMENT with unicode quotes. https://github.com/xnuinside/simple-ddl-parser/pull/308

### Changed
- Updated black dependency to `>=24,<26`. https://github.com/xnuinside/simple-ddl-parser/pull/307
- Switched linting to ruff + black and updated CI/pre-commit checks.

### Fixed
- BigQuery OPTIONS parsing with unicode characters. https://github.com/xnuinside/simple-ddl-parser/issues/298
- MySQL table `CHARACTER SET`/`CHARSET` option parsing. https://github.com/xnuinside/simple-ddl-parser/issues/296
- MySQL composite index columns parsing. https://github.com/xnuinside/simple-ddl-parser/pull/311
- `CREATE DATABASE IF NOT EXISTS` parsing. https://github.com/xnuinside/simple-ddl-parser/issues/293

## [1.8.0] - 2026-01-18
### Deprecated
- Python 3.7 and 3.8 support is deprecated. Minimum supported version is now Python 3.9.

### Added
- Support for Python 3.13.
- Tox configuration for testing across multiple Python versions (3.9-3.13).
- Support for COMMENT ON syntax (TABLE, COLUMN, SCHEMA, DATABASE). https://github.com/xnuinside/simple-ddl-parser/pull/301
- Support for Informix/GBase 8s dialect. https://github.com/xnuinside/simple-ddl-parser/issues/299
  - Informix data types: SERIAL, SERIAL8, BIGSERIAL, INT8, LVARCHAR, TEXT, BYTE, BLOB, CLOB, MONEY, NCHAR, NVARCHAR.
  - DATETIME YEAR TO SECOND/FRACTION/DAY syntax.
  - INTERVAL HOUR TO MINUTE/DAY TO DAY/YEAR TO MONTH syntax.
  - DEFAULT with Informix functions (TODAY, CURRENT, USER).
  - Storage options: IN dbspace, EXTENT SIZE, NEXT SIZE, LOCK MODE.
  - GBase 8s Oracle-compatible types: VARCHAR2, NVARCHAR2, NUMBER.
  - GBase 8s virtual columns: column AS (expression), GENERATED ALWAYS AS (expression).
- Support for ALTER TABLE with multiple column operations (ADD, DROP, MODIFY). https://github.com/xnuinside/simple-ddl-parser/issues/300
  - Multiple ADD/DROP/MODIFY operations in a single ALTER statement (comma-separated).
  - ADD COLUMN syntax (with COLUMN keyword).
  - DROP without COLUMN keyword (Oracle style).

### Changed
- Output for `dropped_columns` and `modified_columns` is now a list instead of a single dict.

### Fixed
- Snowflake sequence NEXTVAL in column default value. https://github.com/xnuinside/simple-ddl-parser/pull/295
- Escape sequence handling for `=` in generated column expressions.
- BigQuery OPTIONS parsing with unicode characters (em-dash, etc.). https://github.com/xnuinside/simple-ddl-parser/issues/298
  - String literals now support any unicode characters in descriptions.
- MySQL COMMENT parsing with unicode curly quotes (U+2018, U+2019). https://github.com/xnuinside/simple-ddl-parser/issues/297
  - Unicode single quotation marks in string literals are now handled correctly.

## [1.7.1] - 2024-10-04
### Fixed
- `character set` issue. https://github.com/xnuinside/simple-ddl-parser/issues/288

## [1.7.0] - 2024-09-30
### Added
- Support for ENUM & SET column types. https://github.com/xnuinside/simple-ddl-parser/issues/259

### Fixed
- DEFAULT value with `::` cast parsing. https://github.com/xnuinside/simple-ddl-parser/issues/286

## [1.6.1] - 2024-08-15
### Fixed
- `CREATE SCHEMA IF NOT EXISTS` plus comment failure. https://github.com/xnuinside/simple-ddl-parser/issues/289
- `schema` or `db.schema` location parsing in Snowflake.

## [1.6.0] - 2024-08-12
### Added
- Athena output mode (initial support). https://github.com/datacontract/datacontract-cli/issues/332

### Changed
- Output changes that can break integrations:
  - All arguments inside brackets are now parsed as separate strings in a list.
    - Example: `file_format = (TYPE=JSON NULL_IF=('field')` was parsed as `'NULL_IF': "('field')"` and now is `'NULL_IF': ["'field'"]`.
  - Added separate tokens for `=` (EQ) and IN (previously parsed as IDs), for contributor/internal use.
  - CHECK statements in columns now parse correctly; IN statements parse as normal lists.
    - Example: `CHECK (include_exclude_ind IN ('I', 'E'))` now produces `{'check': {'constraint_name': 'chk_metalistcombo_logicalopr', 'statement': {'in_statement': {'in': ["'I'", "'E'"], 'name': 'include_exclude_ind'}}}}`.

### Fixed
- DEFAULT word no longer arrives in key `default` in some cases.

## [1.5.4] - 2024-08-11
### Changed
- Snowflake: added `pattern` token for external table statements and improved location rendering.

## [1.5.3] - 2024-08-08
### Fixed
- Snowflake error when `STRIP_OUTER_ARRAY` property is present in `file_format` statements. https://github.com/xnuinside/simple-ddl-parser/issues/276

## [1.5.2] - 2024-07-31
### Added
- MySQL COLLATE support. https://github.com/xnuinside/simple-ddl-parser/pull/266/files

### Fixed
- Snowflake error when `file_format` name is provided. https://github.com/xnuinside/simple-ddl-parser/issues/273

## [1.5.1] - 2024-05-22
### Added
- MySQL INDEX statement in column definitions. https://github.com/xnuinside/simple-ddl-parser/issues/253

## [1.5.0] - 2024-05-19
### Fixed
- `unique` is now set only when there is a single column in a unique constraint/index. https://github.com/xnuinside/simple-ddl-parser/issues/255
- UNIQUE KEY no longer identified as PRIMARY KEY. https://github.com/xnuinside/simple-ddl-parser/issues/253

## [1.4.0] - 2024-05-14
### Added
- Oracle output keywords `temp` and `is_global`; support for CREATE GLOBAL TEMPORARY TABLE. https://github.com/xnuinside/simple-ddl-parser/issues/182

### Fixed
- BigQuery indexes without schema caused issues in BigQuery dialect.

## [1.3.0] - 2024-05-11
### Added
- PostgreSQL with/without time zone support. https://github.com/xnuinside/simple-ddl-parser/issues/250
- BigQuery GENERATE_ARRAY in RANGE_BUCKETS. https://github.com/xnuinside/simple-ddl-parser/issues/183

### Fixed
- PostgreSQL timezone moved from type definition to `with_time_zone` (True/False).
- BigQuery: RANGE in RANGE_BUCKETS is now placed in `range` instead of being parsed as columns.
- BigQuery: second argument of `*_TRUNC` partitions now moved to `trunc_by`.

## [1.2.1] - 2024-05-09
### Added
- MySQL `auto_increment` property in output.
- Oracle constraint name in column definitions. https://github.com/xnuinside/simple-ddl-parser/issues/203
- Oracle GENERATED (ALWAYS | (BY DEFAULT [ON NULL])) AS IDENTITY in column definitions.
- PostgreSQL CAST statement in column GENERATED ALWAYS expressions. https://github.com/xnuinside/simple-ddl-parser/issues/198

### Fixed
- MySQL `auto_increment` in table properties no longer produces empty output. https://github.com/xnuinside/simple-ddl-parser/issues/206

## [1.1.0] - 2024-04-21
### Added
- MySQL INDEX statement inside table definitions.
- MySQL INVISIBLE/VISIBLE support. https://github.com/xnuinside/simple-ddl-parser/issues/243
- Snowflake cluster by statement before columns definition. https://github.com/xnuinside/simple-ddl-parser/issues/234

Older versions are documented in ARCHIVE_CHANGELOG.txt.

[Unreleased]: https://github.com/xnuinside/simple-ddl-parser/compare/1.10.0...HEAD
[1.10.0]: https://github.com/xnuinside/simple-ddl-parser/compare/1.9.0...1.10.0
[1.9.0]: https://github.com/xnuinside/simple-ddl-parser/compare/1.8.0...1.9.0
[1.8.0]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.7.1...1.8.0
[1.7.1]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.7.0...v1.7.1
[1.7.0]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.6.1...v1.7.0
[1.6.1]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.5.4...v1.6.0
[1.5.4]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.5.3...v1.5.4
[1.5.3]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.5.2...v1.5.3
[1.5.2]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/xnuinside/simple-ddl-parser/compare/v1.1.0...v1.2.1
[1.1.0]: https://github.com/xnuinside/simple-ddl-parser/releases/tag/v1.1.0
