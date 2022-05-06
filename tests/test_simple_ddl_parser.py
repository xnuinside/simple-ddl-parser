from simple_ddl_parser import DDLParser


def test_run_postgres_first_query():
    ddl = """
    create table prod.super_table
(
    data_sync_id bigint not null,
    sync_count bigint not null,
    sync_mark timestamp  not  null,
    sync_start timestamp  not null,
    sync_end timestamp  not null,
    message varchar(2000),
    primary key (data_sync_id, sync_start, sync_end, message)
);
    """
    expected = [
        {
            "columns": [
                {
                    "name": "data_sync_id",
                    "type": "bigint",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "sync_count",
                    "type": "bigint",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "sync_mark",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "nullable": False,
                    "size": 2000,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "table_name": "super_table",
            "tablespace": None,
            "schema": "prod",
            "partitioned_by": [],
            "alter": {},
            "checks": [],
            "index": [],
            "primary_key": ["data_sync_id", "sync_start", "sync_end", "message"],
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_run_query_caps_in_columns():
    ddl = """
    CREATE TABLE paths (
        "ID" int PRIMARY KEY,
        "TITLE" varchar NOT NULL,
        "description" varchar(160),
        "created_at" timestamp not null,
        "updated_at" timestamp
    );
    """
    expected = [
        {
            "columns": [
                {
                    "name": '"ID"',
                    "type": "int",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"TITLE"',
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"description"',
                    "type": "varchar",
                    "nullable": True,
                    "size": 160,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"created_at"',
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"updated_at"',
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ['"ID"'],
            "index": [],
            "table_name": "paths",
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
            "alter": {},
            "checks": [],
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_parser_multiple_tables():

    ddl = """

    CREATE TABLE "countries" (
    "id" int PRIMARY KEY,
    "code" varchar(4) NOT NULL,
    "name" varchar NOT NULL
    );

    CREATE TABLE "path_owners" (
    "user_id" int,
    "path_id" int,
    "type" int DEFAULT 1a
    );


    """
    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"code"',
                    "type": "varchar",
                    "size": 4,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"name"',
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ['"id"'],
            "index": [],
            "table_name": '"countries"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
            "alter": {},
            "checks": [],
        },
        {
            "columns": [
                {
                    "name": '"user_id"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"path_id"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"type"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": "1a",
                    "check": None,
                    "references": None,
                    "unique": False,
                },
            ],
            "primary_key": [],
            "index": [],
            "table_name": '"path_owners"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
            "alter": {},
            "checks": [],
        },
    ]

    assert DDLParser(ddl).run() == expected


def test_unique_statement_in_columns():

    ddl = """

    CREATE TABLE "steps" (
    "id" int UNIQUE,
    "title" varchar unique,
    "description" varchar(160),
    "created_at" timestamp,
    "updated_at" timestamp
    );
    """
    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"title"',
                    "type": "varchar",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"description"',
                    "type": "varchar",
                    "size": 160,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"created_at"',
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"updated_at"',
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": '"steps"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
        }
    ]
    assert DDLParser(ddl).run() == expected


def test_unique_statement_separate_line():

    ddl = """

    CREATE TABLE "steps" (
    "id" int,
    "title" varchar,
    "description" varchar(160),
    "created_at" timestamp,
    "updated_at" timestamp,
    unique ("id", "title")
    );
    """
    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"title"',
                    "type": "varchar",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"description"',
                    "type": "varchar",
                    "size": 160,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"created_at"',
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"updated_at"',
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": '"steps"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
        }
    ]

    assert DDLParser(ddl).run() == expected


def test_check_in_column():
    ddl = """
    CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR (50),
        last_name VARCHAR (50),
        birth_date DATE CHECK (birth_date > '1900-01-01'),
        joined_date DATE CHECK (joined_date > birth_date),
        salary numeric CHECK(salary > 0)
        );"""

    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "SERIAL",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "first_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "last_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "birth_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "birth_date > '1900-01-01'",
                },
                {
                    "name": "joined_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "joined_date > birth_date",
                },
                {
                    "name": "salary",
                    "type": "numeric",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "salary > 0",
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": "employees",
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
        }
    ]
    assert expected == DDLParser(ddl).run()


def check_without_constraint():
    ddl = """
CREATE TABLE Persons (
ID int NOT NULL,
LastName varchar(255) NOT NULL,
FirstName varchar(255),
Age int,
City varchar(255),
CHECK (LastName != FirstName)
);
"""
    expected = [
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {},
            "checks": [{"name": None, "statement": "LastName != FirstName"}],
            "table_name": "Persons",
            "schema": None,
            "partitioned_by": [],
        }
    ]

    assert expected == DDLParser(ddl).run()


def test_check_with_constraint():
    ddl = """
    CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int,
    City varchar(255),
    CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes'),
    CHECK (LastName != FirstName)
    );
    """
    expected = [
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {},
            "checks": [
                {
                    "constraint_name": "CHK_Person",
                    "statement": "Age>=18 AND City= 'Sandnes'",
                },
                {"constraint_name": None, "statement": "LastName != FirstName"},
            ],
            "constraints": {
                "checks": [
                    {
                        "constraint_name": "CHK_Person",
                        "statement": "Age>=18 AND City= 'Sandnes'",
                    }
                ]
            },
            "table_name": "Persons",
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_arrays():
    parse_results = DDLParser(
        """
    CREATE table arrays_2 (
        field_1                decimal(21)[] not null
    ,field_2              integer(61) array not null
    ,field_3              varchar array not null default '{"none"}'
    ,squares   integer[3][3] not null default '{1}'
    ,schedule        text[][]
    ,pay_by_quarter  integer[]
    ,pay_by_quarter_2  integer ARRAY[4]
    ,pay_by_quarter_3  integer ARRAY
    ) ;
    """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "field_1",
                    "type": "decimal[]",
                    "size": 21,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_2",
                    "type": "integer[]",
                    "size": 61,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_3",
                    "type": "varchar[]",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'{\"none\"}'",
                    "check": None,
                },
                {
                    "name": "squares",
                    "type": "integer[3][3]",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'{1}'",
                    "check": None,
                },
                {
                    "name": "schedule",
                    "type": "text[][]",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "pay_by_quarter",
                    "type": "integer[]",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "pay_by_quarter_2",
                    "type": "integer[4]",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "pay_by_quarter_3",
                    "type": "integer[]",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": "arrays_2",
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
        }
    ]
    assert expected == parse_results


def test_like_statement():

    ddl = """

    CREATE TABLE New_Users LIKE Old_Users ;
    """

    result = DDLParser(ddl).run()

    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [],
            "index": [],
            "like": {"schema": None, "table_name": "Old_Users"},
            "partitioned_by": [],
            "primary_key": [],
            "schema": None,
            "table_name": "New_Users",
            "tablespace": None,
        }
    ]

    assert expected == result


def test_defaults_with_comments():

    ddl = """

    CREATE table v2.entitlement_requests (
    status                varchar(10) not null default 'requested'  -- inline comment
    ,notes                 varchar(2000) not null default 'none' -- inline comment
    ,id          varchar(100) not null -- inline comment
    ) ;
    """

    result = DDLParser(ddl).run()
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": "'requested'",
                    "name": "status",
                    "nullable": False,
                    "references": None,
                    "size": 10,
                    "type": "varchar",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": "'none'",
                    "name": "notes",
                    "nullable": False,
                    "references": None,
                    "size": 2000,
                    "type": "varchar",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "id",
                    "nullable": False,
                    "references": None,
                    "size": 100,
                    "type": "varchar",
                    "unique": False,
                },
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": "v2",
            "table_name": "entitlement_requests",
            "tablespace": None,
        },
        {"comments": [" inline comment", " inline comment", " inline comment"]},
    ]
    assert expected == result


def test_parse_table_name_table():

    ddl = """
    CREATE TABLE "prefix--schema-name"."table" (
    _id uuid PRIMARY KEY,
    );
    """

    result = DDLParser(ddl).run()

    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "_id",
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "uuid",
                    "unique": False,
                }
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": ["_id"],
            "schema": '"prefix--schema-name"',
            "table_name": '"table"',
            "tablespace": None,
        }
    ]
    assert result == expected


def test_group_by_type_output():
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [
            {
                "cache": 1,
                "increment": 10,
                "maxvalue": 9223372036854775807,
                "minvalue": 0,
                "schema": "dev",
                "sequence_name": "incremental_ids",
                "start": 0,
            }
        ],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "content_type",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": '"schema--notification"."ContentType"',
                        "unique": False,
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": '"schema--notification"',
                "table_name": '"notification"',
                "tablespace": None,
            }
        ],
        "types": [
            {
                "base_type": "ENUM",
                "properties": {"values": ["'TEXT'", "'MARKDOWN'", "'HTML'"]},
                "schema": '"schema--notification"',
                "type_name": '"ContentType"',
            }
        ],
    }

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

    assert result == expected


def test_do_not_fail_on_brackets_in_default():

    ddl = """

    CREATE TABLE "content_filters" (
    "category" int,
    "channels" varchar[],
    "words" varchar[],
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
    );
    """

    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "sequences": [],
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": '"category"',
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": '"channels"',
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "varchar[]",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": '"words"',
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "varchar[]",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "now()",
                        "name": '"created_at"',
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "timestamp",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": '"updated_at"',
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "timestamp",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": '"content_filters"',
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_default_and_primary_inline():

    ddl = """
    CREATE TABLE foo
    (
        entity_id        UUID PRIMARY KEY DEFAULT getId()
    );
    """

    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "entity_id",
                        "type": "UUID",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": "getId()",
                        "check": None,
                    }
                ],
                "primary_key": ["entity_id"],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "foo",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_default_expression():

    ddl = """
    CREATE TABLE foo
    (
        bar_timestamp  bigint DEFAULT 1002 * extract(epoch from now()) * 1000,
        bar_timestamp2  bigint DEFAULT (1002 * extract(epoch from now()) * 1000)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "bar_timestamp",
                        "type": "bigint",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": "1002 * extract(epoch from now()) * 1000",
                        "check": None,
                    },
                    {
                        "name": "bar_timestamp2",
                        "type": "bigint",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": "1002 * extract(epoch from now()) * 1000",
                        "check": None,
                    },
                ],
                "primary_key": [],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "foo",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_comments_in_columns():
    ddl = """
    CREATE TABLE IF NOT EXISTS test_table
    (col1 int PRIMARY KEY COMMENT 'Integer Column',
    col2 string UNIQUE COMMENT 'String Column'
    )
    COMMENT 'This is test table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "if_not_exists": True,
                "columns": [
                    {
                        "name": "col1",
                        "type": "int",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                        "comment": "'Integer Column'",
                    },
                    {
                        "name": "col2",
                        "type": "string",
                        "size": None,
                        "references": None,
                        "unique": True,
                        "nullable": True,
                        "default": None,
                        "check": None,
                        "comment": "'String Column'",
                    },
                ],
                "primary_key": ["col1"],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "test_table",
                "comment": "'This is test table'",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_default_null():
    ddl = """
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT a_d_d(),
  name TEXT DEFAULT NULL
);
"""
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "if_not_exists": True,
                "columns": [
                    {
                        "name": "id",
                        "type": "UUID",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": "a_d_d()",
                        "check": None,
                    },
                    {
                        "name": "name",
                        "type": "TEXT",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": "NULL",
                        "check": None,
                    },
                ],
                "primary_key": ["id"],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "users",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_domains():

    ddl = """
    CREATE DOMAIN domain_1 AS CHAR(10);
    CREATE DOMAIN domain_2 CHAR(16);
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "schemas": [],
        "domains": [
            {
                "schema": None,
                "domain_name": "domain_1",
                "base_type": "CHAR",
                "properties": {},
            },
            {
                "schema": None,
                "domain_name": "DOMAIN",
                "base_type": "CHAR",
                "properties": {},
            },
        ],
    }
    assert expected == result


def test_schema():
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [
            {"schema_name": "bob"},
            {"if_not_exists": True, "schema_name": "schema_name"},
        ],
    }

    ddl = """
    CREATE SCHEMA bob;
    CREATE SCHEMA IF NOT EXISTS schema_name;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_schema_with_authorisation():
    ddl = """
    CREATE SCHEMA AUTHORIZATION joe;
    CREATE SCHEMA new_one AUTHORIZATION user;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [
            {"schema_name": "joe", "authorization": "joe"},
            {"schema_name": "new_one", "authorization": "user"},
        ],
    }
    assert expected == result


def test_generated_always():
    ddl = """
CREATE TABLE people (
    height_cm numeric,
    height_in numeric GENERATED ALWAYS AS (height_cm / 2.54),
    height_in_stored numeric GENERATED ALWAYS AS (height_cm / 2.54) STORED,
);
"""
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "height_cm",
                        "type": "numeric",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "height_in",
                        "type": "numeric",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                        "generated": {
                            "always": True,
                            "as": "height_cm / 2.54",
                            "stored": False,
                        },
                    },
                    {
                        "name": "height_in_stored",
                        "type": "numeric",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                        "generated": {
                            "always": True,
                            "as": "height_cm / 2.54",
                            "stored": True,
                        },
                    },
                ],
                "primary_key": [],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "people",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_generated_always_with_concat():
    ddl = """
    DROP TABLE IF EXISTS contacts;

    CREATE TABLE contacts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        fullname varchar(101) GENERATED ALWAYS AS (CONCAT(first_name,' ',last_name)),
        email VARCHAR(100) NOT NULL
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "INT AUTO_INCREMENT",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "first_name",
                        "type": "VARCHAR",
                        "size": 50,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "last_name",
                        "type": "VARCHAR",
                        "size": 50,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "fullname",
                        "type": "varchar",
                        "size": 101,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                        "generated": {
                            "always": True,
                            "as": "CONCAT(first_name,' ',last_name)",
                            "stored": False,
                        },
                    },
                    {
                        "name": "email",
                        "type": "VARCHAR",
                        "size": 100,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                ],
                "primary_key": ["id"],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "contacts",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_enum_in_lowercase():

    ddl = """
    CREATE TYPE my_status AS enum (
        'NEW',
        'IN_PROGRESS',
        'FINISHED'
    );

    CREATE TABLE foo
    (
        entity_id        UUID PRIMARY KEY DEFAULT getId(),
        status           my_status
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "entity_id",
                        "type": "UUID",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": "getId()",
                        "check": None,
                    },
                    {
                        "name": "status",
                        "type": "my_status",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                ],
                "primary_key": ["entity_id"],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "foo",
            }
        ],
        "types": [
            {
                "schema": None,
                "type_name": "my_status",
                "base_type": "enum",
                "properties": {"values": ["'NEW'", "'IN_PROGRESS'", "'FINISHED'"]},
            }
        ],
        "sequences": [],
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
    }
    assert result == expected


def test_column_names_with_names_like_tokens_works_well():
    ddl = """
    CREATE TABLE foo
    (
        entity_id        UUID PRIMARY KEY DEFAULT getId(),
        key              VARCHAR(256)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "entity_id",
                        "type": "UUID",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": "getId()",
                        "check": None,
                    },
                    {
                        "name": "key",
                        "type": "VARCHAR",
                        "size": 256,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                ],
                "primary_key": ["entity_id"],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "foo",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result


def test_added_create_tablespace():
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tablespaces": [
            {
                "tablespace_name": "tbs1",
                "properties": {"DATAFILE": "'tbs1_data.dbf'", "SIZE": "1m"},
                "type": None,
                "temporary": False,
            }
        ],
    }

    ddl = """
    CREATE TABLESPACE tbs1
    DATAFILE 'tbs1_data.dbf'
    SIZE 1m;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_tablespace_small_big():
    ddl = """
CREATE BIGFILE TABLESPACE tbs_perm_03
  DATAFILE 'tbs_perm_03.dat'
    SIZE 10M
    AUTOEXTEND ON;

CREATE SMALLFILE TABLESPACE tbs_perm_03
  DATAFILE 'tbs_perm_03.dat'
    SIZE 10M
    AUTOEXTEND ON;
"""
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tablespaces": [
            {
                "tablespace_name": "tbs_perm_03",
                "properties": {
                    "DATAFILE": "'tbs_perm_03.dat'",
                    "SIZE": "10M",
                    "AUTOEXTEND": "ON",
                },
                "type": "BIGFILE",
                "temporary": False,
            },
            {
                "tablespace_name": "tbs_perm_03",
                "properties": {
                    "DATAFILE": "'tbs_perm_03.dat'",
                    "SIZE": "10M",
                    "AUTOEXTEND": "ON",
                },
                "type": "SMALLFILE",
                "temporary": False,
            },
        ],
    }
    assert expected == result


def test_tablespaces_temporary():
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tablespaces": [
            {
                "tablespace_name": "tbs_perm_03",
                "properties": {
                    "DATAFILE": "'tbs_perm_03.dat'",
                    "SIZE": "10M",
                    "AUTOEXTEND": "ON",
                },
                "type": "BIGFILE",
                "temporary": True,
            },
            {
                "tablespace_name": "tbs_perm_04",
                "properties": {
                    "DATAFILE": "'tbs_perm_03.dat'",
                    "SIZE": "10M",
                    "AUTOEXTEND": "ON",
                },
                "type": None,
                "temporary": True,
            },
        ],
    }
    ddl = """
    CREATE BIGFILE TEMPORARY TABLESPACE tbs_perm_03
    DATAFILE 'tbs_perm_03.dat'
        SIZE 10M
        AUTOEXTEND ON;

    CREATE TEMPORARY TABLESPACE tbs_perm_04
    DATAFILE 'tbs_perm_03.dat'
        SIZE 10M
        AUTOEXTEND ON;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_create_database():

    ddl = """
    CREATE DATABASE yourdbname;
    CREATE DATABASE "yourdbname2";
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "tables": [],
        "ddl_properties": [],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
        "databases": [
            {"database_name": "yourdbname"},
            {"database_name": '"yourdbname2"'},
        ],
    }
    assert expected == result


def test_collate():
    ddl = """
    CREATE TABLE test1 (
        a text COLLATE "de_DE",
        b text COLLATE "es_ES"
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "collate": '"de_DE"',
                        "default": None,
                        "name": "a",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "text",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "collate": '"es_ES"',
                        "default": None,
                        "name": "b",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "text",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "test1",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_tabs_not_fails_ddl():

    ddl = """
    CREATE TABLE IF NOT EXISTS schema.table
    (
        field_1\tSTRING,
        field_2\tTIMESTAMP
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "if_not_exists": True,
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "field_1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "field_2",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "TIMESTAMP",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": "schema",
                "table_name": "table",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_quotes():

    ddl = """
    CREATE TABLE IF NOT EXISTS `shema`.table
    (
        field_1        BIGINT,
        `partition`   STRING,
    );
    """
    parse_result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "columns": [
                {
                    "name": "field_1",
                    "type": "BIGINT",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`partition`",
                    "type": "STRING",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "tablespace": None,
            "stored_as": None,
            "location": None,
            "comment": None,
            "row_format": None,
            "fields_terminated_by": None,
            "lines_terminated_by": None,
            "map_keys_terminated_by": None,
            "collection_items_terminated_by": None,
            "external": False,
            "schema": "`shema`",
            "table_name": "table",
            "if_not_exists": True,
        }
    ]
    assert expected == parse_result


def test_escaping_symbols_normal_str():
    ddl = """
    CREATE EXTERNAL TABLE test (
    job_id STRING COMMENT 'test\\'s'
    )
    STORED AS PARQUET LOCATION 'hdfs://test'
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "comment": "'test\\'s'",
                        "default": None,
                        "name": "job_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": True,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": "'hdfs://test'",
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": "PARQUET",
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_escaping_symbols_raw_string():
    ddl = r"""
    CREATE EXTERNAL TABLE test (
    job_id STRING COMMENT 'test\'s'
    )
    STORED AS PARQUET LOCATION 'hdfs://test'
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "comment": "'test\\'s'",
                        "default": None,
                        "name": "job_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": True,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": "'hdfs://test'",
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": "PARQUET",
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert result == expected


def test_method_in_check():

    ddl = r"""
    CREATE TABLE foo
    (
        entity_id        UUID PRIMARY KEY DEFAULT getId()
        name             VARCHAR(64),
        CONSTRAINT my_constraint  CHECK(my_function(name) IS TRUE)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [
                    {
                        "constraint_name": "my_constraint",
                        "statement": "my_function(name) IS TRUE",
                    }
                ],
                "columns": [
                    {
                        "check": None,
                        "default": "getId() name VARCHAR(64)",
                        "name": "entity_id",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "UUID",
                        "unique": False,
                    }
                ],
                "constraints": {
                    "checks": [
                        {
                            "constraint_name": "my_constraint",
                            "statement": "my_function(name) IS TRUE",
                        }
                    ]
                },
                "index": [],
                "partitioned_by": [],
                "primary_key": ["entity_id"],
                "schema": None,
                "table_name": "foo",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_ddl_properties():
    ddl = """
    USE [mystaffonline]
    GO
    /****** Object:  Table [dbo].[users_WorkSchedule]    Script Date: 9/29/2021 9:55:26 PM ******/
    SET ANSI_NULLS ON
    GO
    SET QUOTED_IDENTIFIER ON
    GO
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "ddl_properties": [
            {"name": "ANSI_NULLS", "value": "ON"},
            {"name": "QUOTED_IDENTIFIER", "value": "ON"},
        ],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [],
        "types": [],
        "comments": [
            "***** Object:  Table [dbo].[users_WorkSchedule]    Script Date: 9/29/2021 9:55:26 PM ******/"
        ],
    }

    assert result == expected


def test_output_without_separator_in_statements():
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "sid",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "BIGINT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "foo",
                        "nullable": True,
                        "references": None,
                        "size": 5,
                        "type": "CHAR",
                        "unique": False,
                    },
                ],
                "constraints": {
                    "primary_keys": [
                        {"columns": ["sid"], "constraint_name": "sample_key"}
                    ]
                },
                "index": [],
                "partitioned_by": [],
                "primary_key": ["sid"],
                "schema": None,
                "table_name": "sample",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    ddl = """
    DROP TABLE IF EXISTS sample
    CREATE TABLE sample
    (
        sid BIGINT NOT NULL,
        foo CHAR(5),
        CONSTRAINT sample_key PRIMARY KEY NONCLUSTERED (sid)
    )

    """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_lines_starts_with_statement_keys():
    ddl = """
    DROP TABLE IF EXISTS demo

    create TABLE demo
    (
        foo                             char(1),
        CREATE_date                     DATETIME2,
        create                    VARCHAR (20),
        ALTER_date                     DATETIME2,
        alter                    VARCHAR (20),
        DROP_date                    VARCHAR (20),
        drop VARCHAR (20),
    )

    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "foo",
                        "nullable": True,
                        "references": None,
                        "size": 1,
                        "type": "char",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "CREATE_date",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATETIME2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "create",
                        "nullable": True,
                        "references": None,
                        "size": 20,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "ALTER_date",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATETIME2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "alter",
                        "nullable": True,
                        "references": None,
                        "size": 20,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "DROP_date",
                        "nullable": True,
                        "references": None,
                        "size": 20,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "drop",
                        "nullable": True,
                        "references": None,
                        "size": 20,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "demo",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_schema_with_project_name():

    ddl = """
    CREATE SCHEMA IF NOT EXISTS `my.data-cdh-hub`
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [
            {"if_not_exists": True, "project": "my", "schema_name": "data-cdh-hub"}
        ],
        "sequences": [],
        "tables": [],
        "types": [],
    }
    assert expected == result


def test_create_empty_table():

    ddl = """

            CREATE TABLE "material_attachments"
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")

    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [],
                "comment": None,
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": None,
                "table_name": '"material_attachments"',
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_table_name_reserved_word_after_dot():

    ddl = """create table index (col1 int);

    create table foo.[index] (col1 int);

    create table foo.index (col1 int);
        """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "col1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": None,
                "table_name": "index",
                "tablespace": None,
            },
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "col1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": "foo",
                "stored_as": None,
                "table_name": "[index]",
                "tablespace": None,
            },
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "col1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": "foo",
                "stored_as": None,
                "table_name": "index",
                "tablespace": None,
            },
        ],
        "types": [],
    }
    assert expected == result


def test_add_timezone():
    ddl = """
CREATE TABLE foo
        (
            bar_timestamp  bigint DEFAULT 1002 * extract(epoch from now()) * 1000,
            bar_timestamp2  bigint DEFAULT (1002 * extract(epoch from now()) * 1000),
  created_timestamp  TIMESTAMPTZ NOT NULL DEFAULT (now() at time zone 'utc')
        );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": "1002 * extract(epoch from now()) * 1000",
                        "name": "bar_timestamp",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "bigint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "1002 * extract(epoch from now()) * 1000",
                        "name": "bar_timestamp2",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "bigint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "DEFAULT now() at time zone 'utc'",
                        "name": "created_timestamp",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "TIMESTAMPTZ",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "foo",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_check_that_all_columns_parsed_correctly():

    result = DDLParser(
        """CREATE TABLE myset.mytable (
        id_a character varying,
        id_b character varying,
        id_c character varying,
    ); """
    ).run()
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "id_a",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "character varying",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "id_b",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "character varying",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "id_c",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "character varying",
                    "unique": False,
                },
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": "myset",
            "table_name": "mytable",
            "tablespace": None,
        }
    ]
    assert expected == result
