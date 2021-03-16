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
            "schema": "prod",
            "alter": {},
            "checks": [],
            "index": [],
            "primary_key": ["data_sync_id", "sync_start", "sync_end", "message"],
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_run_hql_query():
    ddl = """
    CREATE TABLE "paths" (
        "id" int PRIMARY KEY,
        "title" varchar NOT NULL,
        "description" varchar(160),
        "created_at" timestamp,
        "updated_at" timestamp
    );
    """
    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "int",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "nullable": True,
                    "size": 160,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "table_name": "paths",
            "schema": None,
            "alter": {},
            "checks": [],
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_run_hql_query_caps_in_columns():
    ddl = """
    CREATE TABLE "paths" (
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
                    "name": "ID",
                    "type": "int",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "TITLE",
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "nullable": True,
                    "size": 160,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ["ID"],
            "index": [],
            "table_name": "paths",
            "schema": None,
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
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "code",
                    "type": "varchar",
                    "size": 4,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "table_name": "countries",
            "schema": None,
            "alter": {},
            "checks": [],
        },
        {
            "columns": [
                {
                    "name": "user_id",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "path_id",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "type",
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
            "table_name": "path_owners",
            "schema": None,
            "alter": {},
            "checks": [],
        },
    ]

    assert DDLParser(ddl).run() == expected


def test_references():
    ddl = """
    CREATE table users_events(
    event_id  varchar not null REFERENCES events (id), 
    user_id varchar not null REFERENCES users (id),
    ) ;
    """
    expected = [
        {
            "columns": [
                {
                    "name": "event_id",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": {"table": "events", "schema": None, "column": "id"},
                },
                {
                    "name": "user_id",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": {"table": "users", "schema": None, "column": "id"},
                },
            ],
            "primary_key": [],
            "index": [],
            "table_name": "users_events",
            "schema": None,
            "alter": {},
            "checks": [],
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_references_with_schema():
    ddl = """
    create table prod.super_table
    (
        data_sync_id bigint not null default 0,
        id_ref_from_another_table int REFERENCES other_schema.other_table (id),
        primary key (data_sync_id)
    );

    """
    expected = [
        {
            "columns": [
                {
                    "name": "data_sync_id",
                    "type": "bigint",
                    "size": None,
                    "nullable": False,
                    "default": 0,
                    "references": None,
                    "unique": False,
                    "check": None,
                },
                {
                    "name": "id_ref_from_another_table",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": {
                        "schema": "other_schema",
                        "column": "id",
                        "table": "other_table",
                    },
                },
            ],
            "primary_key": ["data_sync_id"],
            "index": [],
            "table_name": "super_table",
            "schema": "prod",
            "alter": {},
            "checks": [],
        }
    ]

    parse_results = DDLParser(ddl).run()

    assert expected == parse_results


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
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "size": 160,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "updated_at",
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
            "table_name": "steps",
            "schema": None,
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
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "size": None,
                    "references": None,
                    "unique": True,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "size": 160,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "updated_at",
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
            "table_name": "steps",
            "schema": None,
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
            "schema": None,
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
            "checks": [],
            "checks": [{"name": None, "statement": "LastName != FirstName"}],
            "table_name": "Persons",
            "schema": None,
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
            "table_name": "Persons",
            "schema": None,
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
                    "default": "{ none }",
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
            "schema": None,
        }
    ]
    assert expected == parse_results


def test_indexes_in_table():
    parse_results = DDLParser(
        """
    drop table if exists dev.pipeline ;
    CREATE table dev.pipeline (
            job_id               decimal(21) not null
        ,pipeline_id          varchar(100) not null default 'none'
        ,start_time           timestamp not null default now()
        ,end_time             timestamp not null default now()
        ,exitcode             smallint not null default 0
        ,status               varchar(25) not null
        ,elapse_time          float not null default 0
        ,message              varchar(1000) not null default 'none'
        ) ;
    create unique index pipeline_pk on dev.pipeline (job_id) ;
    create index pipeline_ix2 on dev.pipeline (pipeline_id, elapse_time, status) ;        
    """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "job_id",
                    "type": "decimal",
                    "size": 21,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "pipeline_id",
                    "type": "varchar",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'none'",
                    "check": None,
                },
                {
                    "name": "start_time",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "now()",
                    "check": None,
                },
                {
                    "name": "end_time",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "now()",
                    "check": None,
                },
                {
                    "name": "exitcode",
                    "type": "smallint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": 0,
                    "check": None,
                },
                {
                    "name": "status",
                    "type": "varchar",
                    "size": 25,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "elapse_time",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": 0,
                    "check": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "size": 1000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'none'",
                    "check": None,
                },
            ],
            "primary_key": [],
            "alter": {},
            "checks": [],
            "index": [
                {"index_name": "pipeline_pk", "unique": True, "columns": ["job_id"]},
                {
                    "index_name": "pipeline_ix2",
                    "unique": False,
                    "columns": ["pipeline_id", "elapse_time", "status"],
                },
            ],
            "table_name": "pipeline",
            "schema": "dev",
        }
    ]
    assert expected == parse_results


def test_indexes_in_table_wint_no_schema():
    parse_results = DDLParser(
        """
    drop table if exists pipeline ;
    CREATE table pipeline (
            job_id               decimal(21) not null
        ,pipeline_id          varchar(100) not null default 'none'
        ,start_time           timestamp not null default now()
        ,end_time             timestamp not null default now()
        ,exitcode             smallint not null default 0
        ,status               varchar(25) not null
        ,elapse_time          float not null default 0
        ,message              varchar(1000) not null default 'none'
        ) ;
    create unique index pipeline_pk on pipeline (job_id) ;
    create index pipeline_ix2 on pipeline (pipeline_id, elapse_time, status) ;        
    """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "job_id",
                    "type": "decimal",
                    "size": 21,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "pipeline_id",
                    "type": "varchar",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'none'",
                    "check": None,
                },
                {
                    "name": "start_time",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "now()",
                    "check": None,
                },
                {
                    "name": "end_time",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "now()",
                    "check": None,
                },
                {
                    "name": "exitcode",
                    "type": "smallint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": 0,
                    "check": None,
                },
                {
                    "name": "status",
                    "type": "varchar",
                    "size": 25,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "elapse_time",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": 0,
                    "check": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "size": 1000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'none'",
                    "check": None,
                },
            ],
            "primary_key": [],
            "alter": {},
            "checks": [],
            "index": [
                {"index_name": "pipeline_pk", "unique": True, "columns": ["job_id"]},
                {
                    "index_name": "pipeline_ix2",
                    "unique": False,
                    "columns": ["pipeline_id", "elapse_time", "status"],
                },
            ],
            "table_name": "pipeline",
            "schema": None,
        }
    ]
    assert expected == parse_results
