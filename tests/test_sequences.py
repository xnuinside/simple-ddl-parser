from simple_ddl_parser import DDLParser


def test_only_sequence():
    parse_results = DDLParser(
        """

    CREATE SEQUENCE dev.incremental_ids
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;
    """
    ).run()
    expected = [
        {
            "schema": "dev",
            "sequence_name": "incremental_ids",
            "increment": 1,
            "start": 1,
            "minvalue": 1,
            "maxvalue": 9223372036854775807,
            "cache": 1,
        }
    ]
    assert expected == parse_results


def test_sequence_and_table():
    parse_results = DDLParser(
        """

    CREATE SEQUENCE dev.incremental_ids
    INCREMENT 10
    START 0
    MINVALUE 0
    MAXVALUE 9223372036854775807
    CACHE 1;

    CREATE TABLE "countries" (
    "id" int PRIMARY KEY,
    "code" varchar(4) NOT NULL,
    "name" varchar NOT NULL
    );
    """
    ).run()
    expected = [
        {
            "schema": "dev",
            "sequence_name": "incremental_ids",
            "increment": 10,
            "start": 0,
            "minvalue": 0,
            "maxvalue": 9223372036854775807,
            "cache": 1,
        },
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"code"',
                    "type": "varchar",
                    "size": 4,
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
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ['"id"'],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": '"countries"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
        },
    ]
    assert expected == parse_results


def test_sequence_with_by():
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [
            {
                "AS": "[bigint]",
                "cache": True,
                "increment_by": 1,
                "minvalue": 1,
                "schema": "[dbo]",
                "sequence_name": "[sqCdSLIPEvt]",
                "start_with": 1,
            }
        ],
        "tables": [],
        "types": [],
    }

    ddl = """
    CREATE SEQUENCE [dbo].[sqCdSLIPEvt]
    AS [bigint]
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    CACHE
    GO
        """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_add_support_no_value():

    ddl = """
    CREATE SEQUENCE public.accounts_user_id_seq
        AS integer
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [
            {
                "AS": "integer",
                "cache": 1,
                "dataset": "public",
                "increment_by": 1,
                "maxvalue": False,
                "minvalue": False,
                "sequence_name": "accounts_user_id_seq",
                "start_with": 1,
            }
        ],
        "tables": [],
        "types": [],
    }
    assert expected == result
