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
                    "statement": "Age>=18 AND City='Sandnes'",
                },
                {"constraint_name": None, "statement": "LastName != FirstName"},
            ],
            "constraints": {
                "checks": [
                    {
                        "constraint_name": "CHK_Person",
                        "statement": "Age>=18 AND City='Sandnes'",
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
        }
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
            }
        ],
        "types": [],
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
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [{"schema_name": "bob"}, {"schema_name": "schema_name"}],
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
        "sequences": [],
        "domains": [],
        "schemas": [],
    }
    assert expected == result
