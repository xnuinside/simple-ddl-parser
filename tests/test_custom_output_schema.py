import pytest

from simple_ddl_parser import DDLParser, SimpleDDLParserException
from simple_ddl_parser.output.custom_schemas import (
    register_custom_output_schema,
    unregister_custom_output_schema,
)


def test_custom_output_schema_bigquery():
    ddl = """
    CREATE TABLE users (
        id INT NOT NULL,
        email VARCHAR(255)
    );
    """
    result = DDLParser(ddl).run(custom_output_schema="bigquery")
    expected = [
        {
            "table_name": "users",
            "schema": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            ],
        }
    ]
    assert result == expected


def test_custom_output_schema_bigquery_with_dataset_and_project():
    ddl = """
    CREATE TABLE project_id.dataset.users (
        id INT64 NOT NULL
    );
    """
    result = DDLParser(ddl).run(custom_output_schema="bigquery")
    expected = [
        {
            "table_name": "users",
            "project": "project_id",
            "dataset": "dataset",
            "schema": [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}],
        }
    ]
    assert result == expected


def test_custom_output_schema_bigquery_with_array_and_struct():
    ddl = """
    CREATE TABLE users (
        tags ARRAY<STRING>,
        meta STRUCT<key STRING, value INT64>
    );
    """
    result = DDLParser(ddl).run(custom_output_schema="bigquery")
    expected = [
        {
            "table_name": "users",
            "schema": [
                {"name": "tags", "type": "STRING", "mode": "REPEATED"},
                {"name": "meta", "type": "RECORD", "mode": "NULLABLE"},
            ],
        }
    ]
    assert result == expected


def test_custom_output_schema_registration():
    ddl = "CREATE TABLE demo (id INT);"

    def to_custom_schema(output):
        return [{"custom": True, "tables": len(output)}]

    register_custom_output_schema("custom", to_custom_schema)
    try:
        result = DDLParser(ddl).run(custom_output_schema="custom")
        assert result == [{"custom": True, "tables": 1}]
    finally:
        unregister_custom_output_schema("custom")


def test_custom_output_schema_unknown_name():
    ddl = "CREATE TABLE demo (id INT);"
    with pytest.raises(SimpleDDLParserException) as excinfo:
        DDLParser(ddl).run(custom_output_schema="unknown_schema")
    assert "Custom output schema can be one of the following" in str(excinfo.value)
    assert "bigquery" in str(excinfo.value)
