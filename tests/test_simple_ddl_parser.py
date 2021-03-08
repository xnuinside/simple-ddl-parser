import os
from simple_ddl_parser import DDLParser, parse_from_file


def test_run_postgres_first_query():
    ddl = """
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
                    "references": None,
                },
                {
                    "name": "sync_count",
                    "type": "bigint",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "sync_mark",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "nullable": True,
                    "size": 2000,
                    "default": None,
                    "references": None,
                },
            ],
            "table_name": "super_table",
            "schema": "prod",
            "alter": {},
            "primary_key": ["data_sync_id", "sync_start"],
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
                    "references": None,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "nullable": True,
                    "size": 160,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "paths",
            "schema": None,
            "alter": {},
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
                    "references": None,
                },
                {
                    "name": "TITLE",
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "nullable": True,
                    "size": 160,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "nullable": True,
                    "size": None,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["ID"],
            "table_name": "paths",
            "schema": None,
            "alter": {},
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_parse_from_file_one_table():
    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "SERIAL",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": 160,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "country_code",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "default_language",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "users",
            "schema": None,
            "alter": {},
        }
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(os.path.join(current_path, "test_one_table.sql"))


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
                    "references": None,
                },
                {
                    "name": "code",
                    "type": "varchar",
                    "size": 4,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "countries",
            "schema": None,
            "alter": {},
        },
        {
            "columns": [
                {
                    "name": "user_id",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "path_id",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "type",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": "1a",
                    "references": None,
                },
            ],
            "primary_key": [],
            "table_name": "path_owners",
            "schema": None,
            "alter": {},
        },
    ]

    assert DDLParser(ddl).run() == expected


def test_parse_from_file_two_statements():
    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "SERIAL",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "country_code",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "default_language",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "users",
            "schema": None,
            "alter": {},
        },
        {
            "columns": [
                {
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "code",
                    "type": "varchar",
                    "size": 2,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "languages",
            "schema": None,
            "alter": {},
        },
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(
        os.path.join(current_path, "test_two_tables.sql")
    )


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
                    "references": {"table": "events", "schema": None, "column": "id"},
                },
                {
                    "name": "user_id",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": {"table": "users", "schema": None, "column": "id"},
                },
            ],
            "primary_key": [],
            "table_name": "users_events",
            "schema": None,
            "alter": {},
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_references_with_schema():
    ddl = """
    create table prod.super_table
    (
        data_sync_id bigint not null default 0,
        id_ref_from_another_table int REFERENCES other_schema.other_table (id) 
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
                },
                {
                    "name": "id_ref_from_another_table",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": {
                        "schema": "other_schema",
                        "column": "id",
                        "table": "other_table",
                    },
                },
            ],
            "primary_key": ["data_sync_id"],
            "table_name": "super_table",
            "schema": "prod",
            "alter": {},
        }
    ]

    parse_results = DDLParser(ddl).run()

    assert expected == parse_results
