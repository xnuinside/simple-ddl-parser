from simple_ddl_parser import DDLParser


def test_custom_enum():

    ddl = """
    CREATE TYPE "schema--notification"."ContentType" AS
    ENUM ('TEXT','MARKDOWN','HTML');
    CREATE TABLE "schema--notification"."notification" (
        content_type "schema--notification"."ContentType"
    );
    """

    result = DDLParser(ddl).run()
    expected = [
        {
            "base_type": "ENUM",
            "properties": {"values": ["'TEXT'", "'MARKDOWN'", "'HTML'"]},
            "schema": '"schema--notification"',
            "type_name": '"ContentType"',
        },
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
        },
    ]
    assert expected == result


def test_custom_enum_wihtout_schema():

    ddl = """
    CREATE TYPE "ContentType" AS
    ENUM ('TEXT','MARKDOWN','HTML');
    CREATE TABLE "schema--notification"."notification" (
        content_type "ContentType"
    );
    """

    result = DDLParser(ddl).run()
    expected = [
        {
            "base_type": "ENUM",
            "properties": {"values": ["'TEXT'", "'MARKDOWN'", "'HTML'"]},
            "schema": None,
            "type_name": '"ContentType"',
        },
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
                    "type": '"ContentType"',
                    "unique": False,
                }
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": '"schema--notification"',
            "table_name": '"notification"',
        },
    ]
    assert expected == result
