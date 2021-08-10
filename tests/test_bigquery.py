from simple_ddl_parser import DDLParser


def test_dataset_in_output():
    expected = {
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
                        "name": "x",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT64",
                        "unique": False,
                    }
                ],
                "dataset": "mydataset",
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "table_name": "newtable",
                "tablespace": None,
            }
        ],
        "types": [],
    }

    ddl = """
    CREATE TABLE mydataset.newtable ( x INT64 )
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    assert expected == result
