from simple_ddl_parser import DDLParser


def test_spark_sql_using():

    ddl = """CREATE TABLE student (id INT, name STRING, age INT) USING CSV
        COMMENT 'this is a comment'
        TBLPROPERTIES ('foo'='bar');"""
    result = DDLParser(ddl, silent=False, normalize_names=True).run(group_by_type=True)

    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "name",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "age",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "student",
                "tablespace": None,
                "tblproperties": {"'foo'": "'bar'"},
                "using": "CSV",
                "comment": "'this is a comment'",
            }
        ],
        "types": [],
    }
    assert expected == result


def test_partition_by():

    ddl = """CREATE TABLE student (id INT, name STRING, age INT)
        USING CSV
        PARTITIONED BY (age);"""
    result = DDLParser(ddl, silent=False, normalize_names=True).run(group_by_type=True)
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "name",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "age",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": ["age"],
                "primary_key": [],
                "schema": None,
                "table_name": "student",
                "tablespace": None,
                "using": "CSV",
            }
        ],
        "types": [],
    }
    assert expected == result


def test_spark_sql_partitioned_by_function():

    results = DDLParser(
        """
    create table a (b timestamp, c int)
    using iceberg
    partitioned by (months(b))
    location 's3://tables/a'
    """
    ).run(group_by_type=True)

    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "b",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "timestamp",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "c",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    },
                ],
                "index": [],
                "location": "'s3://tables/a'",
                "partitioned_by": [{"args": "(b)", "func_name": "months"}],
                "primary_key": [],
                "schema": None,
                "table_name": "a",
                "tablespace": None,
                "using": "iceberg",
            }
        ],
        "types": [],
    }

    assert expected == results
