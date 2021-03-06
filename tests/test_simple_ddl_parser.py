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
                },
                {
                    "name": "sync_count",
                    "type": "bigint",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "sync_mark",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "nullable": False,
                    "size": 2000,
                    "default": None,
                },
            ],
            "table_name": "super_table",
            "schema": "prod",
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
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "nullable": False,
                    "size": 160,
                    "default": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "paths",
            "schema": None,
        }
    ]
    assert expected == DDLParser(ddl).run()


def test_run_hql_query_caps_in_columns():
    ddl = """
    CREATE TABLE "paths" (
        "ID" int PRIMARY KEY,
        "TITLE" varchar NOT NULL,
        "description" varchar(160),
        "created_at" timestamp,
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
                },
                {
                    "name": "TITLE",
                    "type": "varchar",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "nullable": False,
                    "size": 160,
                    "default": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "nullable": False,
                    "size": None,
                    "default": None,
                },
            ],
            "primary_key": ["ID"],
            "table_name": "paths",
            "schema": None,
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
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": 160,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "country_code",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "default_language",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "users",
            "schema": None,
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
                },
                {
                    "name": "code",
                    "type": "varchar",
                    "size": 4,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "countries",
            "schema": None,
        },
        {
            "columns": [
                {
                    "name": "user_id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "path_id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "type",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": "1a",
                },
            ],
            "primary_key": [],
            "table_name": "path_owners",
            "schema": None,
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
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "country_code",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "default_language",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "users",
            "schema": None,
        },
        {
            "columns": [
                {
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "code",
                    "type": "varchar",
                    "size": 2,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                },
            ],
            "primary_key": ["id"],
            "table_name": "languages",
            "schema": None,
        },
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(
        os.path.join(current_path, "test_two_tables.sql")
    )
