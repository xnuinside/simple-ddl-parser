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
                    "check": "`col_name` IN ('year','month')",
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


def test_checks_with_in_works():

    ddl = """
    CREATE TABLE meta_criteria_combo
    (
    parent_criterion_id NUMBER(3),
    child_criterion_id  NUMBER(3),
    include_exclude_ind CHAR(1) NOT NULL CONSTRAINT chk_metalistcombo_logicalopr CHECK (include_exclude_ind IN ('I', 'E')),
    CONSTRAINT pk_meta_criteria_combo PRIMARY KEY(parent_criterion_id, child_criterion_id),
    CONSTRAINT fk_metacritcombo_parent FOREIGN KEY(parent_criterion_id) REFERENCES meta_criteria ON DELETE CASCADE, 
    CONSTRAINT fk_metacritcombo_child FOREIGN KEY(child_criterion_id) REFERENCES meta_criteria 
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
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
                        "name": "parent_criterion_id",
                        "nullable": False,
                        "references": None,
                        "size": 3,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "child_criterion_id",
                        "nullable": False,
                        "references": None,
                        "size": 3,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": "constraint_name statement",
                        "default": None,
                        "name": "include_exclude_ind",
                        "nullable": False,
                        "references": None,
                        "size": 1,
                        "type": "CHAR",
                        "unique": False,
                    },
                ],
                "constraints": {
                    "primary_keys": [
                        {
                            "columns": ["parent_criterion_id", "child_criterion_id"],
                            "constraint_name": "pk_meta_criteria_combo",
                        }
                    ],
                    "references": [
                        {
                            "columns": [None],
                            "constraint_name": "fk_metacritcombo_parent",
                            "deferrable_initially": None,
                            "on_delete": "CASCADE",
                            "on_update": None,
                            "schema": None,
                            "table": "meta_criteria",
                        },
                        {
                            "columns": [None],
                            "constraint_name": "fk_metacritcombo_child",
                            "deferrable_initially": None,
                            "on_delete": None,
                            "on_update": None,
                            "schema": None,
                            "table": "meta_criteria",
                        },
                    ],
                },
                "index": [],
                "partitioned_by": [],
                "primary_key": ["parent_criterion_id", "child_criterion_id"],
                "schema": None,
                "table_name": "meta_criteria_combo",
                "tablespace": None,
            }
        ],
        "types": [],
    }

    assert result == expected
