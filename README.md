## Simple DDL Parser

Parser tested on DDL for PostgreSQL & Hive. If you have samples that cause an error - please open the issue, I will be glad to fix it.

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

or this


```sql

CREATE TABLE "paths" (
  "id" int PRIMARY KEY,
  "title" varchar NOT NULL,
  "description" varchar(160),
  "created_at" timestamp,
  "updated_at" timestamp
);


```

And produce output like this (information about table name, columns, types and properties):

for the first sql

```json

```


for the second sql

```json


```

## How to install

```bash

    pip install simple-ddl-parser

```


## How to use

### From console 

Only supports parse ddl from file

```bash


```

### From python code

#### To parse files

```python


```

### More examples & tests

You can find in **tests/functional** folder.

### Limitations

Parser does not save your capitalisation in column names & types. By default output ALWAYS has values in Capslock (because of specifik lexx & yacc behavior), like this:

```python

    {'columns': 
        [
            {'name': 'DATA_SYNC_ID', 'type': 'BIGINT', 'mode': False, 'size': None}, {'name': 'SYNC_COUNT', 'type': 'BIGINT', 'mode': False, 'size': None}, ...
        ],
        'primary_key': ['DATA_SYNC_ID', 'SYNC_START']} 
        'table_name': 'DATA_SYNC_HISTORY', 'schema': 'V2'}
```

You can set get output in lower case with argument .run(lower=True)

### Dump result in json

To dump result in json use argument .run(dump=True)

```python



```

You also can provide a path where you want to have a dumps with schema with argument 
### Historical context

This library is an extracted parser code from https://github.com/xnuinside/fakeme (Library for fake relation data generation, that I used in several work projects, but did not have time to make from it normal open source library)

For one of the work projects I needed to convert SQL ddl to Python ORM models in auto way and I tried to use https://github.com/andialbrecht/sqlparse but it works not well enough with ddl for my case (for example, if in ddl used lower case - nothing works, primary keys inside ddl are mapped as column name not reserved word and etc.). So I remembered about Parser in Fakeme and just extracted it & improved. 

## How to contribute

Please describe issue that you want to solve and open the PR, I will review it as soon as possible.
