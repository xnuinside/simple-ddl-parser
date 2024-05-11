from simple_ddl_parser import DDLParser


def test_inherits():
    ddl = """
    CREATE TABLE public."Diagnosis_identifier" (
        "Diagnosis_id" text NOT NULL
    )
    INHERITS (public.identifier);
    """

    result = DDLParser(ddl).run(output_mode="postgres")

    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": '"Diagnosis_id"',
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "text",
                    "unique": False,
                }
            ],
            "index": [],
            "inherits": {"schema": "public", "table_name": "identifier"},
            "partitioned_by": [],
            "primary_key": [],
            "schema": "public",
            "table_name": '"Diagnosis_identifier"',
            "tablespace": None,
        }
    ]
    assert expected == result


def test_cast_generated():
    ddl = """CREATE TABLE test (
      timestamp TIMESTAMP,
      date DATE GENERATED ALWAYS AS (CAST(timestamp AS DATE))
    )"""

    result = DDLParser(ddl).run(output_mode="postgres")
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "timestamp",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "TIMESTAMP",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "generated": {
                        "always": True,
                        "as": {"cast": {"as": "DATE", "value": "timestamp"}},
                        "stored": False,
                    },
                    "name": "date",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "DATE",
                    "unique": False,
                },
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": None,
            "table_name": "test",
            "tablespace": None,
        }
    ]
    assert expected == result


def test_with_time_zone():
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "date_updated",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "timestamp",
                    "unique": False,
                    "with_time_zone": True,
                }
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": "public",
            "table_name": "test",
            "tablespace": None,
        }
    ]
    ddl = """
    CREATE TABLE public.test (date_updated timestamp with time zone);"""

    result = DDLParser(ddl).run(output_mode="postgres")
    assert expected == result
