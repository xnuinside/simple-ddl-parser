from simple_ddl_parser import DDLParser


def test_parse_properties_in_create_db():

    ddl = """
    create database mytestdb2 data_retention_time_in_days = 10 ENCRYPTED = True some_other_property = 'value';
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "databases": [
            {
                "database_name": "mytestdb2",
                "properties": {
                    "ENCRYPTED": "True",
                    "data_retention_time_in_days": "10",
                    "some_other_property": "'value'",
                },
            }
        ],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_create_database_database():
    expected = {
        "databases": [{"database_name": "database"}],
        "ddl_properties": [],
        "domains": [],
        "schemas": [{"schema_name": "SCHEMA"}],
        "sequences": [],
        "tables": [],
        "types": [],
    }

    ddl = """

    CREATE DATABASE database;
    CREATE SCHEMA SCHEMA;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    assert expected == result
