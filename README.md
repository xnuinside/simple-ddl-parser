## Simple DDL Parser

![badge1](https://img.shields.io/pypi/v/simple-ddl-parser) ![badge2](https://img.shields.io/pypi/l/simple-ddl-parser) ![badge3](https://img.shields.io/pypi/pyversions/simple-ddl-parser) 

Parser tested on DDL for PostgreSQL & Hive.

If you have samples that cause an error - please open the issue, I will be glad to fix it.

This parser take as input SQL DDL statements or files, for example like this:

```sql

    create table prod.super_table
(
    data_sync_id bigint not null,
    sync_count bigint not null,
    sync_mark timestamp  not  null,
    sync_start timestamp  not null,
    sync_end timestamp  not null,
    message varchar(2000) null,
    primary key (data_sync_id, sync_start)
);

```

And produce output like this (information about table name, schema, columns, types and properties):

```python

    [{
    'columns': [
        {'name': 'data_sync_id', 'type': 'bigint', 'mode': False, 'size': None, 'default': None}, 
        {'name': 'sync_count', 'type': 'bigint', 'mode': False, 'size': None, 'default': None}, 
        {'name': 'sync_mark', 'type': 'timestamp', 'mode': False, 'size': None, 'default': None}, 
        {'name': 'sync_start', 'type': 'timestamp', 'mode': False, 'size': None, 'default': None}, 
        {'name': 'sync_end', 'type': 'timestamp', 'mode': False, 'size': None, 'default': None}, 
        {'name': 'message', 'type': 'varchar', 'mode': False, 'size': 2000, 'default': None}], 
    'table_name': 'super_table', 'schema': 'prod', 
    'primary_key': ['data_sync_id', 'sync_start']
    }]

```

Or one more example


```sql

CREATE TABLE "paths" (
  "id" int PRIMARY KEY,
  "title" varchar NOT NULL,
  "description" varchar(160),
  "created_at" timestamp,
  "updated_at" timestamp
);


```

and result

```python
        [{
        'columns': [
            {'name': 'id', 'type': 'int', 'nullable': False, 'size': None, 'default': None}, 
            {'name': 'title', 'type': 'varchar', 'nullable': False, 'size': None, 'default': None}, 
            {'name': 'description', 'type': 'varchar', 'nullable': False, 'size': 160, 'default': None}, 
            {'name': 'created_at', 'type': 'timestamp', 'nullable': False, 'size': None, 'default': None}, 
            {'name': 'updated_at', 'type': 'timestamp', 'nullable': False, 'size': None, 'default': None}], 
        'primary_key': ['id'], 
        'table_name': 'paths', 'schema': ''
        }]

```

If you pass file or text block with more when 1 CREATE TABLE statement when result will be list of such dicts. For example:

Input:

```sql

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

```
Output:

```python

    [
        {'columns': [
            {'name': 'id', 'type': 'int', 'size': None, 'nullable': False, 'default': None}, 
            {'name': 'code', 'type': 'varchar', 'size': 4, 'nullable': False, 'default': None}, 
            {'name': 'name', 'type': 'varchar', 'size': None, 'nullable': False, 'default': None}], 
         'primary_key': ['id'], 
         'table_name': 'countries', 
         'schema': None}, 
        {'columns': [
            {'name': 'user_id', 'type': 'int', 'size': None, 'nullable': False, 'default': None}, 
            {'name': 'path_id', 'type': 'int', 'size': None, 'nullable': False, 'default': None}, 
            {'name': 'type', 'type': 'int', 'size': None, 'nullable': False, 'default': 1}], 
         'primary_key': [], 
         'table_name': 'path_owners', 
         'schema': None}
    ]

```

## How to use

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

    result = parse_from_file('tests/test_one_statement.sql')
    print(result)

```

### More examples & tests

You can find in **tests/functional** folder.

### Dump result in json

To dump result in json use argument .run(dump=True)


You also can provide a path where you want to have a dumps with schema with argument

### TODO in next Releases

1. Support for references (Foreigein key) in column defenition
2. Support for separate ALTER TABLE statements for Foreigein keys like

```sql

    ALTER TABLE "material_attachments" ADD FOREIGN KEY ("material_id") REFERENCES "materials" ("id");

```

3. Support for parse CREATE INDEX statements
4. Add command line
5. Support ARRAYs


### Historical context

This library is an extracted parser code from https://github.com/xnuinside/fakeme (Library for fake relation data generation, that I used in several work projects, but did not have time to make from it normal open source library)

For one of the work projects I needed to convert SQL ddl to Python ORM models in auto way and I tried to use https://github.com/andialbrecht/sqlparse but it works not well enough with ddl for my case (for example, if in ddl used lower case - nothing works, primary keys inside ddl are mapped as column name not reserved word and etc.).
So I remembered about Parser in Fakeme and just extracted it & improved. 


## How to contribute

Please describe issue that you want to solve and open the PR, I will review it as soon as possible.

Any questions? Ping me in Telegram: https://t.me/xnuinside 