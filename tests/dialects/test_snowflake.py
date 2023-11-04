from simple_ddl_parser import DDLParser


def test_clone_db():
    ddl = """
    create database mytestdb_clone clone mytestdb;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "databases": [
            {"clone": {"from": "mytestdb"}, "database_name": "mytestdb_clone"}
        ],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [],
        "types": [],
        "ddl_properties": [],
    }
    assert result == expected


def test_clone_table():
    expected = {
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [],
                "index": [],
                "like": {"schema": None, "table_name": "orders"},
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "orders_clone",
                "tablespace": None,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }

    ddl = """
    create table orders_clone clone orders;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_clone_schema():
    expected = {
        "domains": [],
        "schemas": [
            {"clone": {"from": "testschema"}, "schema_name": "mytestschema_clone"}
        ],
        "sequences": [],
        "tables": [],
        "types": [],
        "ddl_properties": [],
    }

    ddl = """
    create schema mytestschema_clone clone testschema;
    """
    result = DDLParser(ddl).run(group_by_type=True)
    assert expected == result


def test_cluster_by():
    ddl = """
    create table mytable (date timestamp_ntz, id number, content variant) cluster by (date, id);
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
                "cluster_by": ["date", "id"],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "date",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "timestamp_ntz",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "number",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "content",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "variant",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "mytable",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_enforced():
    ddl = """
    create table table2 (
        col1 integer not null,
        col2 integer not null,
        constraint pkey_1 primary key (col1, col2) not enforced
        );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "domains": [],
        "ddl_properties": [],
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
                        "name": "col1",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "col2",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "primary_key_enforced": False,
                "schema": None,
                "table_name": "table2",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_table_comment_parsed_validly():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100),
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    ) COMMENT ='ASINs to be excluded from the ASIN List File'
    ;
    """
    result_one = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")

    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100),
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    ) COMMENT='ASINs to be excluded from the ASIN List File'
    ;
    """
    result_two = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")

    expected = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "USER_COMMENT",
                    "nullable": True,
                    "references": None,
                    "size": 100,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "PROCESS_SQN",
                    "nullable": False,
                    "references": None,
                    "size": (10, 0),
                    "type": "NUMBER",
                    "unique": False,
                },
            ],
            "constraints": {
                "primary_keys": [
                    {"columns": ["ASIN"], "constraint_name": "PK_EXCLUSION"}
                ]
            },
            "comment": "'ASINs to be excluded from the ASIN List File'",
            "index": [],
            "partitioned_by": [],
            "primary_key": ["ASIN"],
            "primary_key_enforced": None,
            "schema": "ASIN",
            "table_name": "EXCLUSION",
            "tablespace": None,
        }
    ]

    assert expected == result_one == result_two


def test_schema_parsed_normally():
    ddl = """
    create schema my_schema;
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")

    expected = [{"schema_name": "my_schema"}]

    assert result == expected


def test_comment_on_create_schema():
    ddl = """
    create schema my_schema comment='this is comment1';
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")
    expected = [{"comment": "'this is comment1'", "schema_name": "my_schema"}]
    assert result == expected


def test_table_with_tag():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100) COMMENT 'User input' WITH TAG
        (DBName.MASKING_POLICY_LIBRARY.PROJECT_POLICY_MASK='mask_object'),
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    )
    ;
    """
    result_tagged = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected_tagged = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "USER_COMMENT",
                    "type": "VARCHAR",
                    "size": 100,
                    "comment": "'User input'",
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "with_tag": "DBName.MASKING_POLICY_LIBRARY.PROJECT_POLICY_MASK='mask_object'",
                },
                {
                    "check": None,
                    "default": None,
                    "name": "PROCESS_SQN",
                    "nullable": False,
                    "references": None,
                    "size": (10, 0),
                    "type": "NUMBER",
                    "unique": False,
                },
            ],
            "constraints": {
                "primary_keys": [
                    {"columns": ["ASIN"], "constraint_name": "PK_EXCLUSION"}
                ]
            },
            "index": [],
            "partitioned_by": [],
            "primary_key": ["ASIN"],
            "primary_key_enforced": None,
            "schema": "ASIN",
            "table_name": "EXCLUSION",
            "tablespace": None,
        }
    ]
    f = open("payload.json", "a")
    f.write(str(result_tagged))
    f.close()

    assert result_tagged == expected_tagged


def test_column_with_multiple_tag():
    ddl = """
    create TABLE TABLE_NAME (
        USER_COMMENT VARCHAR(100) COMMENT 'User input' WITH TAG (a.b.c = 'tag1', a.b.d='tag2')
    )
    ;
    """
    result_tagged = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected_tagged = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "check": None,
                    "comment": "'User input'",
                    "default": None,
                    "name": "USER_COMMENT",
                    "nullable": True,
                    "references": None,
                    "size": 100,
                    "type": "VARCHAR",
                    "unique": False,
                    "with_tag": ["a.b.c='tag1'", "a.b.d='tag2'"],
                }
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": None,
            "table_name": "TABLE_NAME",
            "tablespace": None,
        }
    ]
    f = open("payload.json", "a")
    f.write(str(result_tagged))
    f.close()

    assert result_tagged == expected_tagged


def test_table_with_multiple_tag():
    ddl = """
    create TABLE TABLE_NAME (
        COL VARCHAR(100) COMMENT 'User input'
    ) WITH TAG (b.c = 'tag1', b.d='tag2')
    ;
    """
    result_tagged = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected_tagged = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "check": None,
                    "comment": "'User input'",
                    "default": None,
                    "name": "COL",
                    "nullable": True,
                    "references": None,
                    "size": 100,
                    "type": "VARCHAR",
                    "unique": False,
                }
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": None,
            "table_name": "TABLE_NAME",
            "tablespace": None,
            "with_tag": ["b.c='tag1'", "b.d='tag2'"],
        }
    ]
    f = open("payload.json", "a")
    f.write(str(result_tagged))
    f.close()

    assert result_tagged == expected_tagged


def test_table_with_mask():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100) COMMENT 'User input' WITH MASKING POLICY DBName.MASKING_POLICY_LIBRARY.MASK_STRING,
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    )
    ;
    """
    result_masked = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_masked = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "USER_COMMENT",
                    "type": "VARCHAR",
                    "size": 100,
                    "comment": "'User input'",
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "with_masking_policy": "DBName.MASKING_POLICY_LIBRARY.MASK_STRING",
                },
                {
                    "check": None,
                    "default": None,
                    "name": "PROCESS_SQN",
                    "nullable": False,
                    "references": None,
                    "size": (10, 0),
                    "type": "NUMBER",
                    "unique": False,
                },
            ],
            "constraints": {
                "primary_keys": [
                    {"columns": ["ASIN"], "constraint_name": "PK_EXCLUSION"}
                ]
            },
            "index": [],
            "partitioned_by": [],
            "primary_key": ["ASIN"],
            "primary_key_enforced": None,
            "schema": "ASIN",
            "table_name": "EXCLUSION",
            "tablespace": None,
        }
    ]

    assert result_masked == expected_masked


def test_table_with_retention():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100) COMMENT 'User input',
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    ) DATA_RETENTION_TIME_IN_DAYS = 15
    ;
    """
    result_retention = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_retention = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "USER_COMMENT",
                    "type": "VARCHAR",
                    "size": 100,
                    "comment": "'User input'",
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "PROCESS_SQN",
                    "nullable": False,
                    "references": None,
                    "size": (10, 0),
                    "type": "NUMBER",
                    "unique": False,
                },
            ],
            "constraints": {
                "primary_keys": [
                    {"columns": ["ASIN"], "constraint_name": "PK_EXCLUSION"}
                ]
            },
            "index": [],
            "partitioned_by": [],
            "primary_key": ["ASIN"],
            "primary_key_enforced": None,
            "schema": "ASIN",
            "table_name": "EXCLUSION",
            "tablespace": None,
            "data_retention_time_in_days": 15,
        }
    ]

    assert result_retention == expected_retention


def test_table_with_change_tracking():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100) COMMENT 'User input',
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    ) change_tracking = False
    ;
    """
    result_change_tracking = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_change_tracking = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "USER_COMMENT",
                    "type": "VARCHAR",
                    "size": 100,
                    "comment": "'User input'",
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "PROCESS_SQN",
                    "nullable": False,
                    "references": None,
                    "size": (10, 0),
                    "type": "NUMBER",
                    "unique": False,
                },
            ],
            "constraints": {
                "primary_keys": [
                    {"columns": ["ASIN"], "constraint_name": "PK_EXCLUSION"}
                ]
            },
            "index": [],
            "partitioned_by": [],
            "primary_key": ["ASIN"],
            "primary_key_enforced": None,
            "schema": "ASIN",
            "table_name": "EXCLUSION",
            "tablespace": None,
            "change_tracking": False,
        }
    ]

    assert result_change_tracking == expected_change_tracking
