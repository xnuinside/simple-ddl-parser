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
        "domains": [], "ddl_properties": [],
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
        "domains": [], "ddl_properties": [],
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
        "ddl_properties": [],
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
        "domains": [], "ddl_properties": [],
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
        "ddl_properties": [],
    }
    assert expected == result
