from simple_ddl_parser import DDLParser


def test_json_dump_arg():
    ddl = """
    CREATE TABLE list_bucket_single (key STRING, value STRING)
    SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES;
    """
    parse_results = DDLParser(ddl).run(output_mode="hql", json_dump=True)
    expected = (
        '[{"columns": [{"name": "key", "type": "STRING", "size": null, "references": null, '
        '"unique": false, "nullable": true, "default": null, "check": null}, {"name": "value", '
        '"type": "STRING", "size": null, "references": null, "unique": false, "nullable": true, '
        '"default": null, "check": null}], "primary_key": [], "alter": {}, "checks": [], "index": [], '
        '"partitioned_by": [], "tablespace": null, "stored_as": "DIRECTORIES", "location": null, "comment":'
        ' null, "row_format": null, "fields_terminated_by": null, "lines_terminated_by": null,'
        ' "map_keys_terminated_by": null, "collection_items_terminated_by": null, "external": false,'
        ' "schema": null, "table_name": "list_bucket_single", '
        '"skewed_by": {"key": "key", "on": ["1", "5", "6"]}}]'
    )
    assert parse_results == expected
