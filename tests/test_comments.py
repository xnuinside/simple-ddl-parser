from simple_ddl_parser import DDLParser


def test_inline_comment():
    parse_result = DDLParser(
        """
                          drop table if exists user_history ;
    CREATE table user_history (
        runid                 decimal(21) not null
    ,job_id                decimal(21) not null
    ,id                    varchar(100) not null -- group_id or role_id
    ,user              varchar(100) not null
    ,status                varchar(10) not null
    ,event_time            timestamp not null default now()
    ,comment           varchar(1000) not null default 'none'
    ) ;
"""
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "runid",
                    "type": "decimal",
                    "size": 21,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
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
                    "name": "id",
                    "type": "varchar",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "comment": "group_id or role_id",
                },
                {
                    "name": "user",
                    "type": "varchar",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "status",
                    "type": "varchar",
                    "size": 10,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "event_time",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "now()",
                    "check": None,
                },
                {
                    "name": "comment",
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
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "user_history",
            "tablespace": None,
        },
        {"comments": [" group_id or role_id"]},
    ]
    assert expected == parse_result


def test_block_comments():
    ddl = """
        /* outer comment start
        bla bla bla
        /* inner comment */
        select a from b

        outer comment end */
        create table A(/*
                inner comment2 */
            data_sync_id bigint not null ,
            sync_start timestamp  not null,
            sync_end timestamp  not null,
            message varchar(2000),
            primary key (data_sync_id, sync_start, sync_end, message)
        );
        """
    parse_result = DDLParser(ddl).run()
    expected = [
        {
            "columns": [
                {
                    "name": "data_sync_id",
                    "type": "bigint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "size": 2000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["data_sync_id", "sync_start", "sync_end", "message"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "A",
            "tablespace": None,
        },
        {"comments": [" outer comment start", " inner comment */"]},
    ]
    assert expected == parse_result


def test_inline_comments_with_fk_reference():
    ddl = """
    CREATE TABLE pole.t_spiel (
        id varchar(10) NOT NULL, -- Comment 0
        refprodid varchar(10) NOT NULL, -- Comment 1 // references is empty
        titel varchar(100) NOT NULL, -- Comment 2
        datum date NOT NULL,
        uhrzeit time NOT NULL,
        dauer int4 NOT NULL, -- Comment 3, should be 5
        CONSTRAINT t_spiel_pk PRIMARY KEY (id),
        CONSTRAINT foreign_key_t_produktion FOREIGN KEY (refprodid) REFERENCES pole.t_produktion (id)
    );
    """
    result = DDLParser(ddl).run()
    table = result[0]
    columns = {column["name"]: column for column in table["columns"]}

    assert columns["id"]["comment"] == "Comment 0"
    assert columns["refprodid"]["comment"] == "Comment 1 // references is empty"
    assert columns["titel"]["comment"] == "Comment 2"
    assert columns["dauer"]["comment"] == "Comment 3, should be 5"

    assert columns["refprodid"]["references"] == {
        "column": "id",
        "constraint_name": "foreign_key_t_produktion",
        "deferrable_initially": None,
        "on_delete": None,
        "on_update": None,
        "schema": "pole",
        "table": "t_produktion",
    }

    assert result[-1]["comments"]


def test_mysql_comments_support():
    ddl = """
        # this is mysql comment1
    /* outer comment start
    bla bla bla
    /* inner comment */
    select a from b

    outer comment end */
    create table A(/*
            inner comment2 */
        data_sync_id bigint not null ,
        sync_start timestamp  not null,
        sync_end timestamp  not null,
    # this is mysql comment2
        message varchar(2000),
        primary key (data_sync_id, sync_start, sync_end, message)
    );
    """
    parse_result = DDLParser(ddl).run()
    expected = [
        {
            "columns": [
                {
                    "name": "data_sync_id",
                    "type": "bigint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "size": 2000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["data_sync_id", "sync_start", "sync_end", "message"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "A",
            "tablespace": None,
        },
        {"comments": [" outer comment start", " inner comment */"]},
    ]
    assert expected == parse_result


def test_two_defices_in_string_work_ok():
    ddl = """
    CREATE TABLE "my--custom--schema"."users" (
    "id" SERIAL PRIMARY KEY,
    "name" varchar,
    "created_at" timestamp,
    "updated_at" timestamp,
    "country_code" int,
    "default_language" int
    );
    """
    parse_result = DDLParser(ddl).run()

    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "SERIAL",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"name"',
                    "type": "varchar",
                    "size": None,
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
                {
                    "name": '"country_code"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"default_language"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ['"id"'],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": '"my--custom--schema"',
            "partitioned_by": [],
            "table_name": '"users"',
            "tablespace": None,
        }
    ]
    assert expected == parse_result


def test_comment_on_table():
    ddl = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100)
    );

    COMMENT ON TABLE users IS 'User information table';
    """

    parse_result = DDLParser(ddl, silent=False).run()
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
                    "name": "name",
                    "type": "VARCHAR",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["id"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "users",
            "tablespace": None,
            "comment": "User information table",
        }
    ]
    assert expected == parse_result


def test_comment_on_columns_with_special_quotes():
    ddl = """
    CREATE TABLE quoting (
        quote1 VARCHAR(10),
        quote2 VARCHAR(10),
        quote3 VARCHAR(10),
        quote4 VARCHAR(10)
    );

    COMMENT ON COLUMN quoting.quote1 IS 'Column with special quotes: ‘Hello, World!’';
    COMMENT ON COLUMN quoting.quote2 IS 'Column with special quotes: ''Hello, World!''';
    COMMENT ON COLUMN quoting.quote3 IS 'Column with special quotes: “Hello, World!”';
    COMMENT ON COLUMN quoting.quote4 IS 'Column with special quotes: "Hello, World!"';
    """
    parse_result = DDLParser(ddl, silent=False).run()
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "comment": "Column with special quotes: \\u2018Hello, World!\\u2019",
                    "default": None,
                    "name": "quote1",
                    "nullable": True,
                    "references": None,
                    "size": 10,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "comment": "Column with special quotes: 'Hello, World!'",
                    "default": None,
                    "name": "quote2",
                    "nullable": True,
                    "references": None,
                    "size": 10,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "comment": "Column with special quotes: \\u201cHello, World!\\u201d",
                    "default": None,
                    "name": "quote3",
                    "nullable": True,
                    "references": None,
                    "size": 10,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "comment": 'Column with special quotes: "Hello, World!"',
                    "default": None,
                    "name": "quote4",
                    "nullable": True,
                    "references": None,
                    "size": 10,
                    "type": "VARCHAR",
                    "unique": False,
                },
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": None,
            "table_name": "quoting",
            "tablespace": None,
        },
    ]
    assert expected == parse_result


def test_comment_on_columns():
    ddl = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        market VARCHAR(2)
    );

    COMMENT ON COLUMN users.id IS 'Primary key for user identification';
    COMMENT ON COLUMN users.name IS 'User name (first name, last name)';
    COMMENT ON COLUMN users.market IS 'Market code, e.g.
DE
US
IT
PT
UK
IR';
    """

    parse_result = DDLParser(ddl, silent=False).run()
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
                    "comment": "Primary key for user identification",
                },
                {
                    "name": "name",
                    "type": "VARCHAR",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "comment": "User name (first name, last name)",
                },
                {
                    "check": None,
                    "comment": r"Market code, e.g.\nDE\nUS\nIT\nPT\nUK\nIR",
                    "default": None,
                    "name": "market",
                    "nullable": True,
                    "references": None,
                    "size": 2,
                    "type": "VARCHAR",
                    "unique": False,
                },
            ],
            "primary_key": ["id"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "users",
            "tablespace": None,
        }
    ]
    assert expected == parse_result
