from simple_ddl_parser import DDLParser


def test_in_clause_in_check():
    result = DDLParser(
        """create table dev.data_sync_history(
        data_sync_id bigint not null,
        sync_count bigint not null,
        `col_name` varchar(5) CHECK( `col_name` = 'year' OR `col_name` = 'month' ),
        `col_name` varchar(5) CHECK( `col_name` IN ('year', 'month') ),
    ); """
    ).run()

    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "data_sync_id",
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "bigint",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "sync_count",
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "bigint",
                    "unique": False,
                },
                {
                    "check": "`col_name` = 'year' OR `col_name` = 'month'",
                    "default": None,
                    "name": "`col_name`",
                    "nullable": True,
                    "references": None,
                    "size": 5,
                    "type": "varchar",
                    "unique": False,
                },
                {
                    "check": "`col_name` IN ('year', 'month')",
                    "default": None,
                    "name": "`col_name`",
                    "nullable": True,
                    "references": None,
                    "size": 5,
                    "type": "varchar",
                    "unique": False,
                },
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": "dev",
            "table_name": "data_sync_history",
            "tablespace": None,
        }
    ]
    assert result == expected
