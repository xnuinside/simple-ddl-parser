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
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "code",
                    "type": "varchar",
                    "size": 4,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "name",
                    "type": "varchar",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["id"], 'index': [],
            "alter": {},
            "checks": [],
            "table_name": "countries",
            "schema": None,
        },
    ]
    assert expected == parse_results
