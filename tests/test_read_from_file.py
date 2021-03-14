import os
from simple_ddl_parser import parse_from_file


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
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": 160,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "country_code",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "default_language",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ["id"], 'index': [],
            "table_name": "users",
            "schema": None,
            "alter": {},
            "checks": [],
        }
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(
        os.path.join(current_path, "sql", "test_one_table.sql")
    )


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
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "country_code",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": "default_language",
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ["id"], 'index': [],
            "table_name": "users",
            "schema": None,
            "alter": {},
            "checks": [],
        },
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
                    "size": 2,
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
            "primary_key": ["id"], 'index': [],
            "table_name": "languages",
            "schema": None,
            "alter": {},
            "checks": [],
        },
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(
        os.path.join(current_path, "sql", "test_two_tables.sql")
    )
