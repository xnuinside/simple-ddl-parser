from simple_ddl_parser import DDLParser


def test_athena_escaped_by():
    ddl = """
         CREATE EXTERNAL TABLE `database`.`table` (
        column1 string,
        column2 string
    )
    PARTITIONED BY
    (
        column3 integer
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ESCAPED BY '\\'
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION 's3://somewhere-in-s3/prefix1'
    TBLPROPERTIES (
      'parquet.compression'='GZIP'
    )
    """

    expected = [
        {
            "alter": {},
            "checks": [],
            "collection_items_terminated_by": None,
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "column1",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "string",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "column2",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "string",
                    "unique": False,
                },
            ],
            "escaped_by": "\\",
            "external": True,
            "fields_terminated_by": "','",
            "index": [],
            "lines_terminated_by": "'\n'",
            "map_keys_terminated_by": None,
            "partitioned_by": [{"name": "column3", "size": None, "type": "integer"}],
            "primary_key": [],
            "row_format": "DELIMITED",
            "schema": "`database`",
            "stored_as": "TEXTFILE",
            "table_name": "`table`",
            "tablespace": None,
            "tblproperties": {"'parquet.compression'": "'GZIP'"},
            "temp": False,
        }
    ]
    result = DDLParser(ddl, debug=True).run(output_mode="athena")
    assert result == expected
