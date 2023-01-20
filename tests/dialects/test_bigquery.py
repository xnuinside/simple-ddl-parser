import pytest

from simple_ddl_parser import DDLParser

testcases = [
    {
        "test_id": "test_dataset_in_output",
        "ddl": """
        CREATE TABLE mydataset.newtable ( x INT64 );
        """,
        "parsed_ddl": {
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
        },
    },
    {
        "test_id": "test_simple_struct",
        "ddl": """
    CREATE TABLE mydataset.newtable
     (
       x INT64 ,
       y STRUCT<a ARRAY<STRING>,b BOOL>
     )
    """,
        "parsed_ddl": {
            "tables": [
                {
                    "columns": [
                        {
                            "name": "x",
                            "type": "INT64",
                            "size": None,
                            "references": None,
                            "unique": False,
                            "nullable": True,
                            "default": None,
                            "check": None,
                        },
                        {
                            "name": "y",
                            "type": "STRUCT<a ARRAY<STRING>, b BOOL>",
                            "size": None,
                            "references": None,
                            "unique": False,
                            "nullable": True,
                            "default": None,
                            "check": None,
                        },
                    ],
                    "primary_key": [],
                    "alter": {},
                    "checks": [],
                    "index": [],
                    "partitioned_by": [],
                    "tablespace": None,
                    "table_name": "newtable",
                    "dataset": "mydataset",
                }
            ],
            "types": [],
            "ddl_properties": [],
            "sequences": [],
            "domains": [],
            "schemas": [],
        }
    },
    {
        "test_id": "test_schema_options",
        "ddl": """
    CREATE SCHEMA IF NOT EXISTS name-name
    OPTIONS (
    location="path"
    );
    """,
        "parsed_ddl": {
            "ddl_properties": [],
            "domains": [],
            "schemas": [
                {
                    "if_not_exists": True,
                    "properties": {"options": [{"location": '"path"'}]},
                    "schema_name": "name-name",
                }
            ],
            "sequences": [],
            "tables": [],
            "types": [],
        }
    },
    {
        "test_id": "test_two_options_values",
        "ddl": """
    CREATE SCHEMA IF NOT EXISTS name-name
    OPTIONS (
    location="path",
    second_option=second_value
    );
    """,
        "parsed_ddl": {
            "ddl_properties": [],
            "domains": [],
            "schemas": [
                {
                    "properties": {
                        "options": [
                            {"location": '"path"'},
                            {"second_option": "second_value"},
                        ]
                    },
                    "if_not_exists": True,
                    "schema_name": "name-name",
                }
            ],
            "sequences": [],
            "tables": [],
            "types": [],
        }
    }
]

testdata = [
    (testcase["ddl"], testcase["parsed_ddl"])
    for testcase in testcases
]
test_ids = [testcase["test_id"] for testcase in testcases]


@pytest.mark.parametrize("ddl,parsed_ddl", testdata, ids=test_ids)
def test_bigquery(ddl, parsed_ddl):
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    assert result == parsed_ddl


def test_long_string_in_option():
    ddl = """
    CREATE SCHEMA IF NOT EXISTS name-name
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting."
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [
            {
                "properties": {
                    "options": [
                        {
                            "description": '"Calendar table '
                                           "records reference "
                                           "list of calendar "
                                           "dates and related "
                                           "attributes used for "
                                           'reporting."'
                        }
                    ]
                },
                "if_not_exists": True,
                "schema_name": "name-name",
            }
        ],
        "sequences": [],
        "tables": [],
        "types": [],
    }
    assert expected == result


def test_option_in_create_table():
    ddl = """
    CREATE TABLE name.hub.REF_CALENDAR (
    calendar_dt DATE,
    )
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting."
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
                        "name": "calendar_dt",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    }
                ],
                "index": [],
                "options": [
                    {
                        "description": '"Calendar table records reference '
                                       "list of calendar dates and related "
                                       'attributes used for reporting."'
                    }
                ],
                "partitioned_by": [],
                "primary_key": [],
                "schema": "hub",
                "project": "name",
                "table_name": "REF_CALENDAR",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_options_in_column():
    ddl = """
    CREATE TABLE name.hub.REF_CALENDAR (
    calendar_dt DATE OPTIONS(description="Field Description")
    )
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting."
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
                        "name": "calendar_dt",
                        "nullable": True,
                        "options": [{"description": '"Field Description"'}],
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    }
                ],
                "index": [],
                "options": [
                    {
                        "description": '"Calendar table records reference '
                                       "list of calendar dates and related "
                                       'attributes used for reporting."'
                    }
                ],
                "partitioned_by": [],
                "primary_key": [],
                "project": "name",
                "schema": "hub",
                "table_name": "REF_CALENDAR",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_cluster_by_without_brackets():
    ddl = """
    CREATE TABLE name.hub.REF_CALENDAR (
    calendar_dt DATE OPTIONS(description="Field Description")
    )
    CLUSTER BY year_reporting_week_no
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting."
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
                "cluster_by": ["year_reporting_week_no"],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt",
                        "nullable": True,
                        "options": [{"description": '"Field Description"'}],
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    }
                ],
                "index": [],
                "options": [
                    {
                        "description": '"Calendar table records reference '
                                       "list of calendar dates and related "
                                       'attributes used for reporting."'
                    }
                ],
                "partitioned_by": [],
                "primary_key": [],
                "project": "name",
                "schema": "hub",
                "table_name": "REF_CALENDAR",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_two_options_in_create_table():
    ddl = """
    CREATE TABLE mydataset.newtable
    (
    x INT64 OPTIONS(description="An optional INTEGER field")
    )
    OPTIONS(
    expiration_timestamp="2023-01-01 00:00:00 UTC",
    description="a table that expires in 2023",
    )

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
                        "name": "x",
                        "nullable": True,
                        "options": [{"description": '"An optional INTEGER ' 'field"'}],
                        "references": None,
                        "size": None,
                        "type": "INT64",
                        "unique": False,
                    }
                ],
                "index": [],
                "options": [
                    {"expiration_timestamp": '"2023-01-01 00:00:00 UTC"'},
                    {"description": '"a table that expires in 2023"'},
                ],
                "partitioned_by": [],
                "primary_key": [],
                "schema": "mydataset",
                "table_name": "newtable",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_table_name_with_project_id():
    ddl = """
    CREATE SCHEMA IF NOT EXISTS project.calender
    OPTIONS (
    location="project-location"
    );
    CREATE TABLE project_id.calender.REF_CALENDAR (
    calendar_dt DATE,
    calendar_dt_id INT,
    fiscal_half_year_reporting_week_no INT
    )
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting."
    )
    PARTITION BY DATETIME_TRUNC(fiscal_half_year_reporting_week_no, DAY)
    CLUSTER BY calendar_dt



    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [
            {
                "properties": {"options": [{"location": '"project-location"'}]},
                "schema_name": "calender",
                "project": "project",
                "if_not_exists": True,
            }
        ],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "cluster_by": ["calendar_dt"],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "fiscal_half_year_reporting_week_no",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                ],
                "dataset": "calender",
                "index": [],
                "options": [
                    {
                        "description": '"Calendar table records reference '
                                       "list of calendar dates and related "
                                       'attributes used for reporting."'
                    }
                ],
                "partition_by": {
                    "columns": ["fiscal_half_year_reporting_week_no", "DAY"],
                    "type": "DATETIME_TRUNC",
                },
                "partitioned_by": [],
                "primary_key": [],
                "project": "project_id",
                "table_name": "REF_CALENDAR",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_project_id_in_alter_and_references():
    ddl = """
        create TABLE project_id.schema.ChildTableName(
                parentTable varchar
                );
        ALTER TABLE project_id.schema.ChildTableName
        ADD CONSTRAINT "fk_t1_t2_tt"
        FOREIGN KEY ("parentTable")
        REFERENCES project_id.schema.ChildTableName2 ("columnName")
        ON DELETE CASCADE
        ON UPDATE CASCADE;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {
                    "columns": [
                        {
                            "constraint_name": '"fk_t1_t2_tt"',
                            "name": '"parentTable"',
                            "references": {
                                "column": '"columnName"',
                                "deferrable_initially": None,
                                "on_delete": "CASCADE",
                                "on_update": "CASCADE",
                                "project": "project_id",
                                "schema": "schema",
                                "table": "ChildTableName2",
                            },
                        }
                    ]
                },
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "parentTable",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "varchar",
                        "unique": False,
                    }
                ],
                "dataset": "schema",
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "project": "project_id",
                "table_name": "ChildTableName",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_multiple_options():
    ddl = """
    CREATE SCHEMA IF NOT EXISTS project.calender
    OPTIONS (
    location="project-location"
    );
    CREATE TABLE project_id.calender.REF_CALENDAR (
    calendar_dt DATE,
    calendar_dt_id INT,
    fiscal_half_year_reporting_week_no INT
    )
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting.",
    one_more_option = "Option",
    three_options = "Three",
    option_four = "Four")
    PARTITION BY DATETIME_TRUNC(fiscal_half_year_reporting_week_no, DAY)
    CLUSTER BY calendar_dt

    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [
            {
                "properties": {"options": [{"location": '"project-location"'}]},
                "schema_name": "calender",
                "project": "project",
                "if_not_exists": True,
            }
        ],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "cluster_by": ["calendar_dt"],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "fiscal_half_year_reporting_week_no",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                ],
                "dataset": "calender",
                "index": [],
                "options": [
                    {
                        "description": '"Calendar table records reference '
                                       "list of calendar dates and related "
                                       'attributes used for reporting."'
                    },
                    {"one_more_option": '"Option"'},
                    {"three_options": '"Three"'},
                    {"option_four": '"Four"'},
                ],
                "partition_by": {
                    "columns": ["fiscal_half_year_reporting_week_no", "DAY"],
                    "type": "DATETIME_TRUNC",
                },
                "partitioned_by": [],
                "primary_key": [],
                "project": "project_id",
                "table_name": "REF_CALENDAR",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_ars_in_arrays_in_option():
    ddl = """
CREATE TABLE project_id.calender.REF_CALENDAR (
    calendar_dt DATE,
    calendar_dt_id INT,
    fiscal_half_year_reporting_week_no INT
    )
    OPTIONS (
    value_1="some value",
   labels=[("org_unit", "development", "ci")])
        """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
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
                        "name": "calendar_dt",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "fiscal_half_year_reporting_week_no",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                ],
                "dataset": "calender",
                "index": [],
                "options": [
                    {"value_1": '"some value"'},
                    {"labels": ['"org_unit"', '"development"', '"ci"']},
                ],
                "partitioned_by": [],
                "primary_key": [],
                "project": "project_id",
                "table_name": "REF_CALENDAR",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_multiple_options_statements():
    ddl = """
            CREATE TABLE `my.data-cdh-hub-REF-CALENDAR` (
    calendar_dt DATE,
    calendar_dt_id INT
    )
    OPTIONS (
        location="location"
        )
    OPTIONS (
    description="Calendar table records reference list of calendar dates and related attributes used for reporting."
    )
    OPTIONS (
        name ="path"
    )
    OPTIONS (
        kms_two="path",
        two="two two"
    )
    OPTIONS (
        kms_three="path",
        three="three",
        threethree="three three"
    )
    OPTIONS (
        kms_four="path",
        four="four four",
        fourin="four four four",
        fourlast="four four four four"
    );
            """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
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
                        "name": "calendar_dt",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "DATE",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "calendar_dt_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "INT",
                        "unique": False,
                    },
                ],
                "dataset": "`my",
                "index": [],
                "options": [
                    {"location": '"location"'},
                    {
                        "description": '"Calendar table records reference '
                                       "list of calendar dates and related "
                                       'attributes used for reporting."'
                    },
                    {"name": '"path"'},
                    {"kms_two": '"path"'},
                    {"two": '"two two"'},
                    {"kms_three": '"path"'},
                    {"three": '"three"'},
                    {"threethree": '"three three"'},
                    {"kms_four": '"path"'},
                    {"four": '"four four"'},
                    {"fourin": '"four four four"'},
                    {"fourlast": '"four four four four"'},
                ],
                "partitioned_by": [],
                "primary_key": [],
                "table_name": "data-cdh-hub-REF-CALENDAR`",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_bigquery_options_string():
    result = DDLParser(
        """
    CREATE TABLE data.test ( col STRING OPTIONS(description='test') ) OPTIONS(description='test');

    """,
        normalize_names=True,
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
                        "name": "col",
                        "nullable": True,
                        "options": [{"description": "'test'"}],
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "index": [],
                "options": [{"description": "'test'"}],
                "partitioned_by": [],
                "primary_key": [],
                "schema": "data",
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert result == expected
