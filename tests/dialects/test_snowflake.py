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
                "external": False,
                "clone": {"schema": None, "table_name": "orders"},
                "partitioned_by": [],
                "primary_key": [],
                "primary_key_enforced": None,
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
    result = DDLParser(ddl).run(group_by_type=True, output_mode="snowflake")
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
    result = DDLParser(ddl).run(group_by_type=True, output_mode="snowflake")
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
                "clone": None,
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
                "primary_key_enforced": None,
                "schema": None,
                "table_name": "mytable",
                "tablespace": None,
                "external": False,
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
    result = DDLParser(ddl).run(group_by_type=True, output_mode="snowflake")
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "clone": None,
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
                "external": False,
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
            "external": False,
        }
    ]

    assert expected == result_one == result_two


def test_schema_parsed_normally():
    ddl = """
    create schema my_schema_simple;
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")

    expected = [{"schema_name": "my_schema_simple"}]

    assert result == expected


def test_create_schema_if_not_exists():
    ddl = """
    create schema if not exists my_schema_if_not_exists;
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")
    expected = [{"if_not_exists": True, "schema_name": "my_schema_if_not_exists"}]
    assert result == expected


def test_comment_without_space_on_create_schema():
    ddl = """
    create schema my_schema_comment comment='this is my schema''s comment';
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")
    expected = [
        {
            "comment": "'this is my schema''s comment'",
            "schema_name": "my_schema_comment",
        }
    ]
    assert result == expected


def test_comment_on_create_schema():
    ddl = """
    create schema my_schema_comment comment = 'this is my schema''s comment with spaces';
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")
    expected = [
        {
            "comment": "'this is my schema''s comment with spaces'",
            "schema_name": "my_schema_comment",
        }
    ]
    assert result == expected


def test_with_tag_on_create_schema():
    ddl = """
    create schema my_schema_tag with tag(a.b.c='schema_tag1', a.b.d='schema_tag2');
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")
    expected = [
        {
            "schema_name": "my_schema_tag",
            "with_tag": ["a.b.c='schema_tag1'", "a.b.d='schema_tag2'"],
        }
    ]
    assert result == expected


def test_comment_with_tag_on_create_schema():
    ddl = """
    create schema my_schema_tag comment = 'my comment about tags' with tag(a.b.c='schema_tag1', a.b.d='schema_tag2');
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="snowflake")
    expected = [
        {
            "schema_name": "my_schema_tag",
            "comment": "'my comment about tags'",
            "with_tag": ["a.b.c='schema_tag1'", "a.b.d='schema_tag2'"],
        }
    ]
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
            "external": False,
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
            "external": False,
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
            "external": False,
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
            "external": False,
        }
    ]

    assert result_masked == expected_masked


def test_table_with_row_access_policy():
    ddl = """
    CREATE TABLE TEST_TABLE (
        ID INT,
        NAME STRING
    ) WITH ROW ACCESS POLICY MY_POLICY;
    """
    result = DDLParser(ddl, normalize_names=True).run(
        group_by_type=True, output_mode="snowflake"
    )
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "clone": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "ID",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "NAME",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "primary_key_enforced": None,
                "schema": None,
                "table_name": "TEST_TABLE",
                "tablespace": None,
                "external": False,
                "with_row_access_policy": "MY_POLICY",
            }
        ],
        "types": [],
    }
    assert result == expected


def test_table_with_retention():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100) COMMENT 'User input',
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    ) DATA_RETENTION_TIME_IN_DAYS=15
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
            "external": False,
            "table_properties": {"data_retention_time_in_days": 15},
        }
    ]

    assert result_retention == expected_retention


def test_table_with_change_tracking():
    ddl = """
    create TABLE ASIN.EXCLUSION (
        USER_COMMENT VARCHAR(100) COMMENT 'User input',
        PROCESS_SQN NUMBER(10,0) NOT NULL,
        constraint PK_EXCLUSION primary key (ASIN)
    ) change_tracking=False
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
            "external": False,
            "table_properties": {"change_tracking": False},
        }
    ]

    assert result_change_tracking == expected_change_tracking


def test_double_single_quotes():
    # test for https://github.com/xnuinside/simple-ddl-parser/issues/208
    ddl = """CREATE TABLE table (column_1 int comment 'This comment isn''t right')"""
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
                        "comment": "'This comment isn''t right'",
                        "default": None,
                        "name": "column_1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "table",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert result == expected


def test_autoincrement_order():
    # test for https://github.com/xnuinside/simple-ddl-parser/issues/208
    ddl = """CREATE TABLE table (
        surrogatekey_SK NUMBER(38,0) NOT NULL autoincrement start 1 increment 1
        ORDER COMMENT 'Record Identification Number Ordered')"""
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
                        "comment": "'Record Identification Number Ordered'",
                        "default": None,
                        "name": "surrogatekey_SK",
                        "nullable": False,
                        "references": None,
                        "size": (38, 0),
                        "type": "NUMBER",
                        "unique": False,
                        "autoincrement": True,
                        "start": "1",
                        "increment": "1",
                        "increment_order": True,
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "table",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert result == expected


def test_table_with_sequence():
    # test for
    ddl = """CREATE TABLE table (
        surrogatekey_SK NUMBER(38,0) NOT NULL DEFAULT DBTEST.SCTEST.SQTEST.NEXTVAL COMMENT 'Record Identification Number',
        myColumnComment VARCHAR(255) COMMENT 'Record Identification Number from Sequence')"""
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
                        "comment": "'Record Identification Number'",
                        "default": "DBTEST.SCTEST.SQTEST.NEXTVAL",
                        "name": "surrogatekey_SK",
                        "nullable": False,
                        "references": None,
                        "size": (38, 0),
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "comment": "'Record Identification Number from Sequence'",
                        "default": None,
                        "name": "myColumnComment",
                        "nullable": True,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "table",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert result == expected


def test_autoincrement_noorder():
    # test for https://github.com/xnuinside/simple-ddl-parser/issues/208
    ddl = """CREATE TABLE table (
        surrogatekey_SK NUMBER(38,0) NOT NULL autoincrement start 1 increment 1
        NOORDER COMMENT 'Record Identification Number NoOrdered')"""
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
                        "comment": "'Record Identification Number NoOrdered'",
                        "default": None,
                        "name": "surrogatekey_SK",
                        "nullable": False,
                        "references": None,
                        "size": (38, 0),
                        "type": "NUMBER",
                        "unique": False,
                        "autoincrement": True,
                        "start": "1",
                        "increment": "1",
                        "increment_order": False,
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "table",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert result == expected


def test_order_sequence():
    parse_results = DDLParser(
        """

    CREATE SEQUENCE dev.incremental_ids_order
    START WITH 1
    INCREMENT BY 1
    NOORDER;
    """
    ).run()
    expected = [
        {
            "schema": "dev",
            "sequence_name": "incremental_ids_order",
            "increment_by": 1,
            "start_with": 1,
            "noorder": True,
        }
    ]
    assert expected == parse_results


def test_virtual_column_ext_table():
    ddl = """
    create external table if not exists TABLE_DATA_SRC.EXT_PAYLOAD_MANIFEST_WEB (
       "type" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 1), '=', 2 )),
       "year" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 2), '=', 2)),
       "month" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 3), '=', 2)),
       "day" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 4), '=', 2)),
       "cast_YEAR" VARCHAR(200) AS (GET(VALUE,'c1')::string),
       "path" VARCHAR(255) AS (METADATA$FILENAME)
       )
    partition by ("type", "year", "month", "day", "path")
    location=@schema_name.StageName/year=2023/month=08/
    auto_refresh=false
    pattern='*.csv'
    file_format = (TYPE = JSON NULL_IF = () STRIP_OUTER_ARRAY = TRUE )
    ;
    """
    result_ext_table = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_ext_table = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "type",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',1),'=',2)"
                    },
                },
                {
                    "name": "year",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',2),'=',2)"
                    },
                },
                {
                    "name": "month",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',3),'=',2)"
                    },
                },
                {
                    "name": "day",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',4),'=',2)"
                    },
                },
                {
                    "name": "cast_YEAR",
                    "type": "VARCHAR",
                    "size": 200,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "GET(VALUE,'c1')::string"},
                },
                {
                    "name": "path",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "METADATA$FILENAME"},
                },
            ],
            "index": [],
            "partition_by": {
                "columns": ["type", "year", "month", "day", "path"],
                "type": None,
            },
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": "TABLE_DATA_SRC",
            "table_name": "EXT_PAYLOAD_MANIFEST_WEB",
            "tablespace": None,
            "external": True,
            "if_not_exists": True,
            "location": "@schema_name.StageName/year=2023/month=08/",
            "table_properties": {
                "auto_refresh": False,
                "pattern": "'*.csv'",
                "file_format": {
                    "TYPE": "JSON",
                    "NULL_IF": "()",
                    "STRIP_OUTER_ARRAY": "TRUE",
                },
            },
        }
    ]

    assert result_ext_table == expected_ext_table

    location_fm1 = """
    create external table if not exists TABLE_DATA_SRC.EXT_PAYLOAD_MANIFEST_WEB (
       "type" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 1), '=', 2 ))
       )
    location=@StageName
    file_format = (TYPE = JSON NULL_IF = () STRIP_OUTER_ARRAY = TRUE )
    ;
    """
    result_fm1 = DDLParser(location_fm1, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_fm1 = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "type",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',1),'=',2)"
                    },
                }
            ],
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": "TABLE_DATA_SRC",
            "table_name": "EXT_PAYLOAD_MANIFEST_WEB",
            "tablespace": None,
            "external": True,
            "if_not_exists": True,
            "index": [],
            "location": "@StageName",
            "table_properties": {
                "file_format": {
                    "TYPE": "JSON",
                    "NULL_IF": "()",
                    "STRIP_OUTER_ARRAY": "TRUE",
                },
            },
        }
    ]

    assert result_fm1 == expected_fm1

    location_fm2 = """
    create external table if not exists TABLE_DATA_SRC.EXT_PAYLOAD_MANIFEST_WEB (
       "type" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 1), '=', 2 )),
       )
    location = @db.schema.StageName/year=2024
    file_format = (TYPE = JSON NULL_IF = () STRIP_OUTER_ARRAY = TRUE )
    ;
    """
    result_fm2 = DDLParser(location_fm2, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_fm2 = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "type",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',1),'=',2)"
                    },
                }
            ],
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": "TABLE_DATA_SRC",
            "table_name": "EXT_PAYLOAD_MANIFEST_WEB",
            "tablespace": None,
            "external": True,
            "if_not_exists": True,
            "index": [],
            "location": "@db.schema.StageName/year=2024",
            "table_properties": {
                "file_format": {
                    "TYPE": "JSON",
                    "NULL_IF": "()",
                    "STRIP_OUTER_ARRAY": "TRUE",
                },
            },
        }
    ]

    assert result_fm2 == expected_fm2

    location_fm3 = """
    create external table if not exists TABLE_DATA_SRC.EXT_PAYLOAD_MANIFEST_WEB (
       "type" VARCHAR(255) AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 1), '=', 2 ))
       )
    location=@Db.Schema.StageName/year=2024/
    file_format = (TYPE = JSON NULL_IF = () STRIP_OUTER_ARRAY = TRUE )
    ;
    """
    result_fm3 = DDLParser(location_fm3, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_fm3 = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "type",
                    "type": "VARCHAR",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "SPLIT_PART(SPLIT_PART(METADATA$FILENAME,'/',1),'=',2)"
                    },
                }
            ],
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": "TABLE_DATA_SRC",
            "table_name": "EXT_PAYLOAD_MANIFEST_WEB",
            "tablespace": None,
            "external": True,
            "if_not_exists": True,
            "index": [],
            "location": "@Db.Schema.StageName/year=2024/",
            "table_properties": {
                "file_format": {
                    "TYPE": "JSON",
                    "NULL_IF": "()",
                    "STRIP_OUTER_ARRAY": "TRUE",
                },
            },
        }
    ]

    assert result_fm3 == expected_fm3


def test_virtual_column_table():
    ddl = """
    create or replace table if not exists TABLE_DATA_SRC.EXT_PAYLOAD_MANIFEST_WEB (
       id bigint,
       derived bigint as (id * 10),
       "year" NUMBER(38,0) AS (EXTRACT(year from METADATA$FILE_LAST_MODIFIED)),
       PERIOD VARCHAR(200) AS (CAST(col1 AS VARCHAR(16777216))),
       field VARCHAR(205) AS (CAST(GET(VALUE, 'c3') AS VARCHAR(16777216)))
       )
    location = @sc.stage/entity=events/
    auto_refresh = false
    file_format = (TYPE=JSON NULL_IF=('field') DATE_FORMAT=AUTO TRIM_SPACE=TRUE)
    stage_file_format = (TYPE=JSON NULL_IF=())
    ;
    """
    result_ext_table = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )

    expected_ext_table = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "name": "id",
                    "type": "bigint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "derived",
                    "type": "bigint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "id * 10"},
                },
                {
                    "name": "year",
                    "type": "NUMBER",
                    "size": (38, 0),
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {
                        "as": "EXTRACT(year from METADATA$FILE_LAST_MODIFIED)"
                    },
                },
                {
                    "name": "PERIOD",
                    "type": "VARCHAR",
                    "size": 200,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "CAST(col1ASVARCHAR(16777216))"},
                },
                {
                    "name": "field",
                    "type": "VARCHAR",
                    "size": 205,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "CAST(GET(VALUE,'c3')ASVARCHAR(16777216))"},
                },
            ],
            "index": [],
            "external": False,
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": "TABLE_DATA_SRC",
            "table_name": "EXT_PAYLOAD_MANIFEST_WEB",
            "tablespace": None,
            "replace": True,
            "if_not_exists": True,
            "location": "@sc.stage/entity=events/",
            "table_properties": {
                "auto_refresh": False,
                "file_format": {
                    "TYPE": "JSON",
                    "NULL_IF": ["'field'"],
                    "DATE_FORMAT": "AUTO",
                    "TRIM_SPACE": "TRUE",
                },
                "stage_file_format": {"TYPE": "JSON", "NULL_IF": "()"},
            },
        }
    ]

    assert result_ext_table == expected_ext_table


def test_schema_create():
    ddl = """
    create schema myschema;
    """
    result = DDLParser(ddl).run(output_mode="snowflake")
    expected = [{"schema_name": "myschema"}]

    assert expected == result


def test_schema_create_if_not_exists():
    ddl = """
    create schema if not exists myschema;
    """
    result = DDLParser(ddl).run(output_mode="snowflake")
    expected = [{"schema_name": "myschema", "if_not_exists": True}]

    assert expected == result


def test_schema_create_if_not_exists_options():
    ddl = """
    create schema if not exists myschema comment = 'mycomment'  tag (demo = 'test');
    """
    schema_if_not_exists = DDLParser(ddl).run(output_mode="snowflake")
    expected = [
        {
            "if_not_exists": True,
            "schema_name": "myschema",
            "comment": "'mycomment'",
            "with_tag": "demo='test'",
        }
    ]

    assert schema_if_not_exists == expected


def test_schema_create_or_replace():
    # https://docs.snowflake.com/en/sql-reference/sql/create-schema
    ddl = """
    create or replace schema myschema;
    """
    result = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected = [{"schema_name": "myschema"}]

    assert result == expected


def test_external_table_with_nullif():
    ddl = """create or replace external table if not exists ${database_name}.MySchemaName.MyTableName(
            "Filename" VARCHAR(16777216) AS (METADATA$FILENAME))
            partition by ("Filename")
            location = @ADL_DH_DL_PTS/
            auto_refresh = false
            file_format = (TYPE=JSON NULLIF=())
            ;"""

    result = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected = [
        {
            "table_name": "MyTableName",
            "schema": "MySchemaName",
            "primary_key": [],
            "columns": [
                {
                    "name": "Filename",
                    "type": "VARCHAR",
                    "size": 16777216,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "METADATA$FILENAME"},
                }
            ],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "partition_by": {"columns": ["Filename"], "type": None},
            "tablespace": None,
            "if_not_exists": True,
            "table_properties": {
                "project": "${database_name}",
                "auto_refresh": False,
                "file_format": {"TYPE": "JSON", "NULLIF": "()"},
            },
            "replace": True,
            "location": "@ADL_DH_DL_PTS/",
            "external": True,
            "primary_key_enforced": None,
            "clone": None,
        }
    ]

    assert result == expected


def test_external_table_file_format_without_parenthesis():
    ddl = """create or replace external table if not exists ${database_name}.MySchemaName.MyTableName(
            "Filename" VARCHAR(16777216) AS (METADATA$FILENAME))
            partition by ("Filename")
            location = @ADL_DH_DL_PTS/
            auto_refresh = false
            file_format = MyFormatName
            ;"""

    result = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected = [
        {
            "table_name": "MyTableName",
            "schema": "MySchemaName",
            "primary_key": [],
            "columns": [
                {
                    "name": "Filename",
                    "type": "VARCHAR",
                    "size": 16777216,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "METADATA$FILENAME"},
                }
            ],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "partition_by": {"columns": ["Filename"], "type": None},
            "tablespace": None,
            "if_not_exists": True,
            "table_properties": {
                "project": "${database_name}",
                "auto_refresh": False,
                "file_format": "MyFormatName",
            },
            "replace": True,
            "location": "@ADL_DH_DL_PTS/",
            "external": True,
            "primary_key_enforced": None,
            "clone": None,
        }
    ]

    assert result == expected


def test_external_table_with_field_delimiter():
    ddl = """create or replace external table if not exists ${database_name}.MySchemaName.MyTableName(
            "Filename" VARCHAR(16777216) AS (METADATA$FILENAME))
            partition by ("Filename")
            location = @ADL_DH_DL_PTS/
            auto_refresh = false
            file_format = (TYPE=CSV FIELD_DELIMITER='|'
            TRIM_SPACE=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE REPLACE_INVALID_CHARACTERS=TRUE)
            ;"""

    result = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected = [
        {
            "table_name": "MyTableName",
            "schema": "MySchemaName",
            "primary_key": [],
            "columns": [
                {
                    "name": "Filename",
                    "type": "VARCHAR",
                    "size": 16777216,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "generated": {"as": "METADATA$FILENAME"},
                }
            ],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "partition_by": {"columns": ["Filename"], "type": None},
            "tablespace": None,
            "if_not_exists": True,
            "table_properties": {
                "project": "${database_name}",
                "auto_refresh": False,
                "file_format": {
                    "TYPE": "CSV",
                    "FIELD_DELIMITER": "'|'",
                    "TRIM_SPACE": "TRUE",
                    "ERROR_ON_COLUMN_COUNT_MISMATCH": "FALSE",
                    "REPLACE_INVALID_CHARACTERS": "TRUE",
                },
            },
            "replace": True,
            "location": "@ADL_DH_DL_PTS/",
            "external": True,
            "primary_key_enforced": None,
            "clone": None,
        }
    ]

    assert result == expected


def test_table_column_def_clusterby():
    ddl = """CREATE TABLE ${database_name}.MySchemaName."MyTableName"
    (ID NUMBER(38,0) NOT NULL, "DocProv" VARCHAR(2)) cluster by ("DocProv");"""

    result = DDLParser(ddl, normalize_names=True, debug=True).run(
        output_mode="snowflake"
    )
    expected = [
        {
            "table_name": "MyTableName",
            "schema": "MySchemaName",
            "primary_key": [],
            "columns": [
                {
                    "name": "ID",
                    "size": (38, 0),
                    "type": "NUMBER",
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "DocProv",
                    "size": 2,
                    "type": "VARCHAR",
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "cluster_by": ["DocProv"],
            "tablespace": None,
            "external": False,
            "primary_key_enforced": None,
            "table_properties": {"project": "${database_name}"},
            "clone": None,
        }
    ]

    assert result == expected


def test_add_clustered_by_before_columns_statement():
    ddl = """CREATE TABLE ${database_name}.MySchemaName."MyTableName"
    cluster by ("DocProv") (
    ID NUMBER(38,0) NOT NULL,
    "DocProv" VARCHAR(2)
    );"""

    result = DDLParser(ddl).run(output_mode="snowflake")
    expected = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "cluster_by": ['"DocProv"'],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "ID",
                    "nullable": False,
                    "references": None,
                    "size": (38, 0),
                    "type": "NUMBER",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": '"DocProv"',
                    "nullable": True,
                    "references": None,
                    "size": 2,
                    "type": "VARCHAR",
                    "unique": False,
                },
            ],
            "external": False,
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "schema": "MySchemaName",
            "table_name": '"MyTableName"',
            "table_properties": {"project": "${database_name}"},
            "tablespace": None,
        }
    ]
    assert result == expected


def test_create_secure_view_with_columns_and_properties():
    ddl = """
    CREATE OR REPLACE SECURE VIEW IF NOT EXISTS analytics.mart.v_orders (
        order_id WITH MASKING POLICY gov.policies.mask_id COMMENT 'Order id',
        region WITH TAG (gov.tags.pii = 'none'),
        amount
    )
      WITH ROW ACCESS POLICY gov.policies.rap_region ON (region)
      CHANGE_TRACKING = TRUE
      COPY GRANTS
      COMMENT = 'Orders view'
    AS SELECT o.order_id, o.region, o.amount
    FROM raw.sales.orders o
    WHERE o.amount > 0;
    """
    result = DDLParser(ddl, silent=False).run(
        output_mode="snowflake", group_by_type=True
    )
    order_source = {"database": "raw", "schema": "sales", "table_name": "orders"}
    assert result["tables"] == []
    assert result["views"] == [
        {
            "schema": "mart",
            "view_name": "v_orders",
            "database": "analytics",
            "replace": True,
            "if_not_exists": True,
            "secure": True,
            "columns": [
                {
                    "name": "order_id",
                    "with_masking_policy": "gov.policies.mask_id",
                    "comment": "'Order id'",
                    "expression": "o.order_id",
                    "source": {**order_source, "column": "order_id"},
                },
                {
                    "name": "region",
                    "with_tag": "gov.tags.pii='none'",
                    "expression": "o.region",
                    "source": {**order_source, "column": "region"},
                },
                {
                    "name": "amount",
                    "expression": "o.amount",
                    "source": {**order_source, "column": "amount"},
                },
            ],
            "with_row_access_policy": "gov.policies.rap_region",
            "row_access_policy_on": ["region"],
            "change_tracking": True,
            "copy_grants": True,
            "comment": "'Orders view'",
            "definition": "SELECT o.order_id, o.region, o.amount "
            "FROM raw.sales.orders o WHERE o.amount > 0",
            "source_tables": [order_source],
        }
    ]


def test_create_view_query_lineage_with_ctes_joins_and_subqueries():
    ddl = """
    CREATE VIEW mart.v_customer_sales AS
    WITH orders_cte AS (
        SELECT customer_id, amount, order_date
        FROM raw.sales.orders
        WHERE EXTRACT(YEAR FROM order_date) > 2020
    )
    SELECT c.customer_id,
           c.name customer_name,
           SUM(oc.amount) AS total_amount,
           MIN(oc.order_date)::DATE first_order,
           CASE WHEN c.vip THEN 'Y' ELSE 'N' END vip_flag,
           (SELECT COUNT(*) FROM raw.sales.returns rt
            WHERE rt.customer_id = c.customer_id) AS returns
    FROM raw.crm.customers AS c
    JOIN orders_cte oc ON oc.customer_id = c.customer_id,
    LATERAL FLATTEN(input => c.tags) f
    GROUP BY 1, 2;
    """
    view = DDLParser(ddl, silent=False).run(group_by_type=True)["views"][0]
    customers = {"database": "raw", "schema": "crm", "table_name": "customers"}
    assert view["source_tables"] == [
        {"database": "raw", "schema": "sales", "table_name": "orders"},
        {"database": "raw", "schema": "sales", "table_name": "returns"},
        customers,
    ]
    assert view["columns"] == [
        {
            "name": "customer_id",
            "expression": "c.customer_id",
            "source": {**customers, "column": "customer_id"},
        },
        {
            "name": "customer_name",
            "expression": "c.name",
            "source": {**customers, "column": "name"},
        },
        {"name": "total_amount", "expression": "SUM(oc.amount)", "source": None},
        {
            "name": "first_order",
            "expression": "MIN(oc.order_date)::DATE",
            "source": None,
        },
        {
            "name": "vip_flag",
            "expression": "CASE WHEN c.vip THEN 'Y' ELSE 'N' END",
            "source": None,
        },
        {
            "name": "returns",
            "expression": "(SELECT COUNT(*) FROM raw.sales.returns rt "
            "WHERE rt.customer_id = c.customer_id)",
            "source": None,
        },
    ]


def test_create_dynamic_table():
    ddl = """
    CREATE OR REPLACE TRANSIENT DYNAMIC TABLE db.sch.dt_orders (
        order_id NUMBER(38, 0),
        amount NUMBER(10, 2) COMMENT 'Amount'
    )
      TARGET_LAG = '20 minutes'
      WAREHOUSE = compute_wh
      REFRESH_MODE = INCREMENTAL
      INITIALIZE = ON_CREATE
      CLUSTER BY (order_id)
      DATA_RETENTION_TIME_IN_DAYS = 7
      COMMENT = 'Dynamic orders'
    AS SELECT order_id, amount FROM db.sch.orders;
    """
    result = DDLParser(ddl, silent=False).run(output_mode="snowflake")
    order_source = {"database": "db", "schema": "sch", "table_name": "orders"}
    assert result == [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "cluster_by": ["order_id"],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "order_id",
                    "nullable": True,
                    "references": None,
                    "size": (38, 0),
                    "type": "NUMBER",
                    "unique": False,
                    "expression": "order_id",
                    "source": {**order_source, "column": "order_id"},
                },
                {
                    "check": None,
                    "comment": "'Amount'",
                    "default": None,
                    "name": "amount",
                    "nullable": True,
                    "references": None,
                    "size": (10, 2),
                    "type": "NUMBER",
                    "unique": False,
                    "expression": "amount",
                    "source": {**order_source, "column": "amount"},
                },
            ],
            "comment": "'Dynamic orders'",
            "dynamic": True,
            "external": False,
            "index": [],
            "initialize": "ON_CREATE",
            "partitioned_by": [],
            "primary_key": [],
            "primary_key_enforced": None,
            "query": "SELECT order_id, amount FROM db.sch.orders",
            "refresh_mode": "INCREMENTAL",
            "replace": True,
            "schema": "sch",
            "source_tables": [order_source],
            "table_name": "dt_orders",
            "table_properties": {"data_retention_time_in_days": 7, "project": "db"},
            "tablespace": None,
            "target_lag": "20 minutes",
            "transient": True,
            "warehouse": "compute_wh",
        }
    ]


def test_create_dynamic_table_without_column_types():
    ddl = """
    CREATE DYNAMIC TABLE IF NOT EXISTS dt_simple (id COMMENT 'pk', label)
      TARGET_LAG = DOWNSTREAM
      WAREHOUSE = wh
      WITH TAG (a.b.c = 'x', a.b.d = 'y')
      AS (SELECT id, UPPER(label) AS label FROM src);
    """
    table = DDLParser(ddl, silent=False).run(output_mode="snowflake")[0]
    assert table["dynamic"] is True
    assert table["if_not_exists"] is True
    assert table["target_lag"] == "DOWNSTREAM"
    assert table["warehouse"] == "wh"
    assert table["with_tag"] == ["a.b.c='x'", "a.b.d='y'"]
    assert [(c["name"], c["type"], c.get("comment")) for c in table["columns"]] == [
        ("id", None, "'pk'"),
        ("label", None, None),
    ]
    assert table["query"] == "(SELECT id, UPPER(label) AS label FROM src)"
    assert [(c["expression"], c["source"]) for c in table["columns"]] == [
        ("id", {"table_name": "src", "schema": None, "column": "id"}),
        ("UPPER(label)", None),
    ]
    assert table["source_tables"] == [{"table_name": "src", "schema": None}]


def test_create_dynamic_table_columns_from_query():
    ddl = """
    CREATE DYNAMIC TABLE dt_from_query
      TARGET_LAG = '1 hour'
      WAREHOUSE = wh
      AS SELECT s.id, s.amount * 2 AS double_amount FROM sch.src s;
    """
    table = DDLParser(ddl, silent=False).run(output_mode="snowflake")[0]
    src = {"schema": "sch", "table_name": "src"}
    assert table["columns"] == [
        {
            "name": "id",
            "type": None,
            "size": None,
            "references": None,
            "unique": False,
            "nullable": True,
            "default": None,
            "check": None,
            "expression": "s.id",
            "source": {**src, "column": "id"},
        },
        {
            "name": "double_amount",
            "type": None,
            "size": None,
            "references": None,
            "unique": False,
            "nullable": True,
            "default": None,
            "check": None,
            "expression": "s.amount * 2",
            "source": None,
        },
    ]


def test_create_materialized_view():
    ddl = """
    CREATE OR REPLACE SECURE MATERIALIZED VIEW IF NOT EXISTS db.mart.mv_daily_sales
      COPY GRANTS
      (sale_date COMMENT 'Day', total_amount WITH TAG (gov.tags.level = 'fin'))
      COMMENT = 'Daily sales'
      CLUSTER BY (sale_date)
    AS SELECT s.sale_date, SUM(s.amount)
    FROM db.raw.sales s
    GROUP BY s.sale_date;

    CREATE MATERIALIZED VIEW mv_all AS SELECT * FROM t;
    """
    result = DDLParser(ddl, silent=False).run(
        output_mode="snowflake", group_by_type=True
    )
    sales = {"database": "db", "schema": "raw", "table_name": "sales"}
    assert result["tables"] == []
    assert result["views"] == [
        {
            "schema": "mart",
            "view_name": "mv_daily_sales",
            "database": "db",
            "replace": True,
            "if_not_exists": True,
            "secure": True,
            "materialized": True,
            "copy_grants": True,
            "comment": "'Daily sales'",
            "cluster_by": ["sale_date"],
            "columns": [
                {
                    "name": "sale_date",
                    "comment": "'Day'",
                    "expression": "s.sale_date",
                    "source": {**sales, "column": "sale_date"},
                },
                {
                    "name": "total_amount",
                    "with_tag": "gov.tags.level='fin'",
                    "expression": "SUM(s.amount)",
                    "source": None,
                },
            ],
            "definition": "SELECT s.sale_date, SUM(s.amount) "
            "FROM db.raw.sales s GROUP BY s.sale_date",
            "source_tables": [sales],
        },
        {
            "schema": None,
            "view_name": "mv_all",
            "replace": False,
            "materialized": True,
            "columns": [
                {
                    "name": "*",
                    "expression": "*",
                    "source": {"table_name": "t", "schema": None, "column": "*"},
                }
            ],
            "definition": "SELECT * FROM t",
            "source_tables": [{"table_name": "t", "schema": None}],
        },
    ]


def test_create_transient_table_keeps_transient_flag():
    ddl = "CREATE TRANSIENT TABLE sch.t (id NUMBER);"
    result = DDLParser(ddl, silent=False).run(output_mode="snowflake")
    assert result[0]["transient"] is True
