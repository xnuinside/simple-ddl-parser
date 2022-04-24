from simple_ddl_parser import DDLParser


def test_partitioned_by_hql_output_mode_hql():
    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS database.table_name
    (
        day_long_nm     string,
        calendar_dt     date,
        source_batch_id string,
        field_qty       decimal(10, 0),
        field_bool      boolean,
        field_float     float,
        create_tmst     timestamp,
        field_double    double,
        field_long      bigint
    ) PARTITIONED BY (batch_id int);

    CREATE TABLE IF NOT EXISTS database.table_name2
    (
        day_long_nm     string,
        calendar_dt     date,
        source_batch_id string,
        field_qty       decimal(10, 0),
        field_bool      boolean,
        field_float     float,
        create_tmst     timestamp,
        field_double    double,
        field_long      bigint
    ) PARTITIONED BY (batch_id int)

    """

    result = DDLParser(ddl).run(output_mode="hql")

    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "day_long_nm",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "calendar_dt",
                    "type": "date",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "source_batch_id",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_qty",
                    "type": "decimal",
                    "size": (10, 0),
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_bool",
                    "type": "boolean",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_float",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "create_tmst",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_long",
                    "type": "bigint",
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
            "partitioned_by": [{"name": "batch_id", "type": "int", "size": None}],
            "external": True,
            "stored_as": None,
            "schema": "database",
            "table_name": "table_name",
            "tablespace": None,
            "location": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        },
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "day_long_nm",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "calendar_dt",
                    "type": "date",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "source_batch_id",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_qty",
                    "type": "decimal",
                    "size": (10, 0),
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_bool",
                    "type": "boolean",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_float",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "create_tmst",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_long",
                    "type": "bigint",
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
            "partitioned_by": [{"name": "batch_id", "type": "int", "size": None}],
            "external": False,
            "schema": "database",
            "stored_as": None,
            "table_name": "table_name2",
            "tablespace": None,
            "location": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        },
    ]

    assert expected == result


def test_stored_as_hql_showed():
    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS database.table_name
    (
        day_long_nm     string,
        calendar_dt     date,
        source_batch_id string,
        field_qty       decimal(10, 0),
        field_bool      boolean,
        field_float     float,
        create_tmst     timestamp,
        field_double    double,
        field_long      bigint
    ) PARTITIONED BY (batch_id int) STORED AS PARQUET

    """

    result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "day_long_nm",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "calendar_dt",
                    "type": "date",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "source_batch_id",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_qty",
                    "type": "decimal",
                    "size": (10, 0),
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_bool",
                    "type": "boolean",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_float",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "create_tmst",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_long",
                    "type": "bigint",
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
            "partitioned_by": [{"name": "batch_id", "type": "int", "size": None}],
            "external": True,
            "schema": "database",
            "table_name": "table_name",
            "tablespace": None,
            "stored_as": "PARQUET",
            "location": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]
    assert expected == result


def test_location_showed():
    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS database.table_name
    (
        day_long_nm     string,
        calendar_dt     date,
        source_batch_id string,
        field_qty       decimal(10, 0),
        field_bool      boolean,
        field_float     float,
        create_tmst     timestamp,
        field_double    double,
        field_long      bigint
    ) PARTITIONED BY (batch_id int) STORED AS PARQUET LOCATION 's3://datalake/table_name/v1'

    """

    result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "day_long_nm",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "calendar_dt",
                    "type": "date",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "source_batch_id",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_qty",
                    "type": "decimal",
                    "size": (10, 0),
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_bool",
                    "type": "boolean",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_float",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "create_tmst",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_long",
                    "type": "bigint",
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
            "partitioned_by": [{"name": "batch_id", "type": "int", "size": None}],
            "stored_as": "PARQUET",
            "location": "'s3://datalake/table_name/v1'",
            "external": True,
            "schema": "database",
            "table_name": "table_name",
            "tablespace": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]
    assert expected == result


def partitioned_by_multiple_tables_hql():

    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS database.table_name
    (
        day_long_nm     string,
        calendar_dt     date,
        source_batch_id string,
        field_qty       decimal(10, 0),
        field_bool      boolean,
        field_float     float,
        create_tmst     timestamp,
        field_double    double,
        field_long      bigint
    ) PARTITIONED BY (batch_id int, batch_id2 string, batch_32 some_type) STORED AS PARQUET LOCATION 's3://datalake/table_name/v1'

    """  # noqa E501

    result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "columns": [
                {
                    "name": "day_long_nm",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "calendar_dt",
                    "type": "date",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "source_batch_id",
                    "type": "string",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_qty",
                    "type": "decimal",
                    "size": (10, 0),
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_bool",
                    "type": "boolean",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_float",
                    "type": "float",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "create_tmst",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_double",
                    "type": "double",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "field_long",
                    "type": "bigint",
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
            "partitioned_by": [
                {"name": "batch_id", "type": "int", "size": None},
                {"name": "batch_id2", "type": "string", "size": None},
                {"name": "batch_32", "type": "some_type", "size": None},
            ],
            "stored_as": "PARQUET",
            "location": "'s3://datalake/table_name/v1'",
            "external": True,
            "schema": "database",
            "table_name": "table_name",
            "tablespace": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]
    assert expected == result


def test_hql_row_format():
    ddl = """
    CREATE TABLE IF NOT EXISTS default.salesorderdetail(
            SalesOrderID int,
            ProductID int,
            OrderQty int,
            LineTotal decimal
            )
        ROW FORMAT DELIMITED
        STORED AS TEXTFILE
    """

    result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "SalesOrderID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "ProductID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "OrderQty",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LineTotal",
                    "type": "decimal",
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
            "stored_as": "TEXTFILE",
            "location": None,
            "external": False,
            "schema": "default",
            "table_name": "salesorderdetail",
            "tablespace": None,
            "row_format": "DELIMITED",
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]
    assert expected == result


def test_fields_terminated_by_hql():

    ddl = """
    CREATE TABLE IF NOT EXISTS default.salesorderdetail(
            SalesOrderID int,
            ProductID int,
            OrderQty int,
            LineTotal decimal
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """

    result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "SalesOrderID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "ProductID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "OrderQty",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LineTotal",
                    "type": "decimal",
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
            "stored_as": "TEXTFILE",
            "location": None,
            "row_format": "DELIMITED",
            "fields_terminated_by": "','",
            "external": False,
            "schema": "default",
            "table_name": "salesorderdetail",
            "tablespace": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]
    assert expected == result


def test_collection_items_terminated_by_hql():

    ddl = """
    CREATE TABLE IF NOT EXISTS default.salesorderdetail(
            SalesOrderID int,
            ProductID int,
            OrderQty int,
            LineTotal decimal
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\002'
            COLLECTION ITEMS TERMINATED BY '\002'
        STORED AS TEXTFILE
    """

    result = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "SalesOrderID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "ProductID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "OrderQty",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LineTotal",
                    "type": "decimal",
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
            "stored_as": "TEXTFILE",
            "location": None,
            "row_format": "DELIMITED",
            "fields_terminated_by": "'\\002'",
            "external": False,
            "schema": "default",
            "table_name": "salesorderdetail",
            "tablespace": None,
            "collection_items_terminated_by": "'\\002'",
            "map_keys_terminated_by": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]
    assert expected == result


def test_map_keys_terminated_by_hql():

    ddl = """
    CREATE TABLE IF NOT EXISTS default.salesorderdetail(
            SalesOrderID int,
            ProductID int,
            OrderQty int,
            LineTotal decimal
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            COLLECTION ITEMS TERMINATED BY '\002'
            MAP KEYS TERMINATED BY '\003'
        STORED AS TEXTFILE
    """

    result = DDLParser(ddl).run(output_mode="hql")

    expected = [
        {
            "if_not_exists": True,
            "columns": [
                {
                    "name": "SalesOrderID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "ProductID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "OrderQty",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LineTotal",
                    "type": "decimal",
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
            "stored_as": "TEXTFILE",
            "location": None,
            "row_format": "DELIMITED",
            "fields_terminated_by": "','",
            "external": False,
            "schema": "default",
            "table_name": "salesorderdetail",
            "tablespace": None,
            "collection_items_terminated_by": "'\\002'",
            "map_keys_terminated_by": "'\\003'",
            "lines_terminated_by": None,
            "comment": None,
        }
    ]

    assert expected == result


def simple_structure_type_support():

    ddl = """
    CREATE TABLE IF NOT EXISTS default.salesorderdetail(
            column_abc ARRAY<structcolx:string,coly:string>
            )
    """

    result = DDLParser(ddl).run(output_mode="hql")

    expected = [
        {
            "alter": {},
            "checks": [],
            "collection_items_terminated_by": None,
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "column_abc",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "ARRAY<structcolx:stringcoly:string>",
                    "unique": False,
                }
            ],
            "external": False,
            "fields_terminated_by": None,
            "index": [],
            "location": None,
            "map_keys_terminated_by": None,
            "partitioned_by": [],
            "primary_key": [],
            "row_format": None,
            "schema": "default",
            "stored_as": None,
            "table_name": "salesorderdetail",
            "tablespace": None,
            "lines_terminated_by": None,
            "comment": None,
        }
    ]

    assert expected == result


def test_complex_structure_test_hql():
    ddl = """
    CREATE TABLE IF NOT EXISTS default.salesorderdetail(
            column_abc ARRAY <structcolx:string,coly:string>,
            employee_info STRUCT < employer: STRING, id: BIGINT, address: STRING >,
            employee_description string,
            column_abc2 ARRAY<structcolx:string,coly:string>,
            column_map MAP < STRING, STRUCT < year: INT, place: STRING, details: STRING >>,
            column_map_no_spaces MAP<STRING,STRUCT<year:INT,place:STRING,details:STRING>>,
            column_struct STRUCT < street_address: STRUCT <street_number: INT, street_name: STRING, street_type: STRING>, country: STRING, postal_code: STRING > not null
            )
    """  # noqa E501

    result = DDLParser(ddl).run(output_mode="hql", group_by_type=True)
    expected = {
        "tables": [
            {
                "if_not_exists": True,
                "columns": [
                    {
                        "name": "column_abc",
                        "type": "ARRAY <structcolx:string, coly:string>",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "employee_info",
                        "type": "STRUCT < employer: STRING, id: BIGINT, address: STRING >",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "employee_description",
                        "type": "string",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "column_abc2",
                        "type": "ARRAY<structcolx:string, coly:string>",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "column_map",
                        "type": "MAP < STRING, STRUCT < year: INT, place: STRING, details: STRING >>",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "column_map_no_spaces",
                        "type": "MAP<STRING, STRUCT<year:INT, place:STRING, details:STRING>>",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "column_struct",
                        "type": "STRUCT < street_address: STRUCT <street_number: INT, "
                        "street_name: STRING, street_type: STRING>, country: STRING, postal_code: STRING >",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
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
                "stored_as": None,
                "location": None,
                "comment": None,
                "row_format": None,
                "fields_terminated_by": None,
                "lines_terminated_by": None,
                "map_keys_terminated_by": None,
                "collection_items_terminated_by": None,
                "external": False,
                "schema": "default",
                "table_name": "salesorderdetail",
            }
        ],
        "types": [],
        "ddl_properties": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
    }

    assert expected == result


def test_comment_and_lines():
    ddl = """
    CREATE TABLE IF NOT EXISTS employee ( eid int, name String,
    salary String, destination String)
    COMMENT ‘Employee details’
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ‘\t’
    LINES TERMINATED BY ‘\n’
    STORED AS TEXTFILE;
    """

    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tables": [
            {
                "if_not_exists": True,
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "eid",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "name",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "String",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "salary",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "String",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "destination",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "String",
                        "unique": False,
                    },
                ],
                "comment": "'Employee details'",
                "external": False,
                "fields_terminated_by": "'\t'",
                "index": [],
                "lines_terminated_by": "'\n'",
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": "DELIMITED",
                "schema": None,
                "stored_as": "TEXTFILE",
                "table_name": "employee",
                "tablespace": None,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_simple_serde():

    ddl = """
    CREATE TABLE apachelog (
    host STRING,
    identity STRING,
    user STRING,
    time STRING,
    request STRING,
    status STRING,
    size STRING,
    referer STRING,
    agent STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    STORED AS TEXTFILE;
    """

    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "sequences": [],
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "host",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "identity",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "user",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "time",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "request",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "status",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "size",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "referer",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "agent",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                ],
                "comment": None,
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": {
                    "java_class": "'org.apache.hadoop.hive.serde2.RegexSerDe'",
                    "serde": True,
                },
                "schema": None,
                "stored_as": "TEXTFILE",
                "table_name": "apachelog",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_with_serde_properties():

    ddl = """
    CREATE TABLE apachelog (
    host STRING,
    identity STRING,
    user STRING,
    time STRING,
    request STRING,
    status STRING,
    size STRING,
    referer STRING,
    agent STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
    "input.regex" = "([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*)
    (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?")
    STORED AS TEXTFILE;
    """

    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "host",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "identity",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "user",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "time",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "request",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "status",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "size",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "referer",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "agent",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                ],
                "comment": None,
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": {
                    "java_class": "'org.apache.hadoop.hive.serde2.RegexSerDe'",
                    "properties": {
                        "parse_m_input_regex": ' "([^]*) '
                        "([^]*) "
                        "([^]*) "
                        "(-|\\\\[^\\\\]*\\\\]) "
                        "([^ "
                        '"]*|"[^"]*") '
                        "(-|[0-9]*)\\n    "
                        "(-|[0-9]*)(?: "
                        "([^ "
                        '"]*|".*") '
                        "([^ "
                        '"]*|".*"))?"'
                    },
                    "serde": True,
                },
                "schema": None,
                "stored_as": "TEXTFILE",
                "table_name": "apachelog",
                "tablespace": None,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_comment_without_null_statement():
    ddl = """
    CREATE EXTERNAL TABLE test (
    job_id STRING COMMENT 'test'
    )
    STORED AS PARQUET LOCATION 'hdfs://test'
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "job_id",
                        "comment": "'test'",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": True,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": "'hdfs://test'",
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": "PARQUET",
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_special_characters_in_comment():
    expected = {
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "comment": "'t# est | & * % $ // * 6 % !?;;\\0b1\\0a7@~^'",
                        "default": None,
                        "name": "job_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": True,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": "'hdfs://test'",
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": "PARQUET",
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    ddl = """
    CREATE EXTERNAL TABLE test (
    job_id STRING COMMENT 't# est | & * % $ // * 6 % !?;;±§@~^'
    )
    STORED AS PARQUET LOCATION 'hdfs://test'
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    assert expected == result


def test_partitioned_by_multiple_columns():
    ddl = """
    CREATE EXTERNAL TABLE test (
    test STRING NULL COMMENT 'xxxx',
    )
    PARTITIONED BY (snapshot STRING, cluster STRING)
    """
    parse_result = DDLParser(ddl).run(output_mode="hql", group_by_type=True)
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "comment": "'xxxx'",
                        "default": None,
                        "name": "test",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": True,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [
                    {"name": "snapshot", "size": None, "type": "STRING"},
                    {"name": "cluster", "size": None, "type": "STRING"},
                ],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": None,
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert parse_result == expected


def test_table_properties():
    ddl = """
    CREATE EXTERNAL TABLE test (
    job_id STRING COMMENT 'test'
    )
    STORED AS PARQUET LOCATION 'hdfs://test'
    TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'parquet.compression2'='SNAPPY2',
    'parquet.compression3'='SNAPPY3',
    'parquet.compression4'='SNAPPY4'
    )
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "comment": "'test'",
                        "default": None,
                        "name": "job_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    }
                ],
                "comment": None,
                "external": True,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": "'hdfs://test'",
                "map_keys_terminated_by": None,
                "partitioned_by": [],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": "PARQUET",
                "table_name": "test",
                "tablespace": None,
                "tblproperties": {
                    "'parquet.compression'": "'SNAPPY'",
                    "'parquet.compression2'": "'SNAPPY2'",
                    "'parquet.compression3'": "'SNAPPY3'",
                    "'parquet.compression4'": "'SNAPPY4'",
                },
            }
        ],
        "types": [],
    }
    assert expected == result


def test_output_input_format():
    ddl = """
    CREATE EXTERNAL TABLE test (
    test STRING NULL COMMENT 'xxxx',
    )
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'hdfs://xxxx'
    """
    parse_results = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "columns": [
                {
                    "name": "test",
                    "type": "STRING",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "comment": "'xxxx'",
                }
            ],
            "primary_key": [],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "tablespace": None,
            "stored_as": {
                "outputformat": "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'",
                "inputformat": "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'",
            },
            "location": "'hdfs://xxxx'",
            "comment": None,
            "row_format": {
                "serde": True,
                "java_class": "'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'",
            },
            "fields_terminated_by": None,
            "lines_terminated_by": None,
            "map_keys_terminated_by": None,
            "collection_items_terminated_by": None,
            "external": True,
            "schema": None,
            "table_name": "test",
        }
    ]
    assert expected == parse_results


def test_skewed_by():
    ddl = """
    CREATE TABLE list_bucket_single (key STRING, value STRING)
      SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES;
    """
    parse_results = DDLParser(ddl).run(output_mode="hql")
    expected = [
        {
            "columns": [
                {
                    "name": "key",
                    "type": "STRING",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "value",
                    "type": "STRING",
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
            "stored_as": "DIRECTORIES",
            "location": None,
            "comment": None,
            "row_format": None,
            "fields_terminated_by": None,
            "lines_terminated_by": None,
            "map_keys_terminated_by": None,
            "collection_items_terminated_by": None,
            "external": False,
            "schema": None,
            "table_name": "list_bucket_single",
            "skewed_by": {"key": "key", "on": ["1", "5", "6"]},
        }
    ]
    assert expected == parse_results


def test_allow_use_tags_in_column_names():

    ddl = """
        CREATE TABLE IF NOT EXISTS default.salesorderdetail(
                something<2% ARRAY<structcolx:string,coly:string>
                )"""
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "if_not_exists": True,
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "something<2%",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "ARRAY<structcolx:string, coly:string>",
                        "unique": False,
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": "default",
                "table_name": "salesorderdetail",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_clustered():
    ddl = """
        CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
    COMMENT 'A bucketed copy of user_info'
    PARTITIONED BY(ds STRING)
    CLUSTERED BY(user_id)
    CLUSTERED ON(user_id);
        """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "clustered_by": ["user_id"],
                "clustered_on": ["user_id"],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "user_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "BIGINT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "firstname",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "lastname",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                ],
                "comment": "'A bucketed copy of user_info'",
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [{"name": "ds", "size": None, "type": "STRING"}],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": None,
                "table_name": "user_info_bucketed",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_into_buckets():

    ddl = """
        CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
    COMMENT 'A bucketed copy of user_info'
    PARTITIONED BY(ds STRING)
    CLUSTERED BY(user_id) INTO 256 BUCKETS;
        """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "clustered_by": ["user_id"],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "user_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "BIGINT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "firstname",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "lastname",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                ],
                "comment": "'A bucketed copy of user_info'",
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "into_buckets": "256",
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [{"name": "ds", "size": None, "type": "STRING"}],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": None,
                "table_name": "user_info_bucketed",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_clustered_by_multiple_columns():

    ddl = """

    set hive.enforce.bucketing = true;
        CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
    COMMENT 'A bucketed copy of user_info'
    PARTITIONED BY(ds STRING)
    CLUSTERED BY(user_id, new_column, one_more_column) INTO 256 BUCKETS;
        """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "ddl_properties": [{"name": "hive.enforce.bucketing", "value": "true"}],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "clustered_by": ["user_id", "new_column", "one_more_column"],
                "collection_items_terminated_by": None,
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "user_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "BIGINT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "firstname",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "lastname",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "STRING",
                        "unique": False,
                    },
                ],
                "comment": "'A bucketed copy of user_info'",
                "external": False,
                "fields_terminated_by": None,
                "index": [],
                "into_buckets": "256",
                "lines_terminated_by": None,
                "location": None,
                "map_keys_terminated_by": None,
                "partitioned_by": [{"name": "ds", "size": None, "type": "STRING"}],
                "primary_key": [],
                "row_format": None,
                "schema": None,
                "stored_as": None,
                "table_name": "user_info_bucketed",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_hql_create_remote_schema():

    ddl = """

    CREATE REMOTE SCHEMA one;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [{"properties": "one", "remote": True, "schema_name": "one"}],
        "sequences": [],
        "tables": [],
        "types": [],
    }
    assert expected == result


def test_remote_database():

    ddl = """

    CREATE REMOTE DATABASE one;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "databases": [{"database_name": "one", "remote": True}],
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [],
        "types": [],
    }
    assert expected == result


def test_multiple_serde_options():
    ddl = """CREATE TABLE `x` (`a` STRING) ROW FORMAT SERDE 'my_serde'
WITH SERDEPROPERTIES ( 'key1'='value1', 'key2'='value2' , 'key3'='value3' , 'keyN'='valueN' )"""

    result = DDLParser(ddl, normalize_names=True).run(output_mode="hql")
    expected = [
        {
            "alter": {},
            "checks": [],
            "collection_items_terminated_by": None,
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "name": "a",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "STRING",
                    "unique": False,
                }
            ],
            "comment": None,
            "external": False,
            "fields_terminated_by": None,
            "index": [],
            "lines_terminated_by": None,
            "location": None,
            "map_keys_terminated_by": None,
            "partitioned_by": [],
            "primary_key": [],
            "row_format": {
                "java_class": "'my_serde'",
                "properties": {
                    "'key1'": "'value1'",
                    "'key2'": "'value2'",
                    "'key3'": "'value3'",
                    "'keyN'": "'valueN'",
                },
                "serde": True,
            },
            "schema": None,
            "stored_as": None,
            "table_name": "x",
            "tablespace": None,
        }
    ]
    assert result == expected


def test_location_with_table_properties_in_like():

    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS schema.specific_table LIKE
    schema.table_template LOCATION "/path/to/table"
    TBLPROPERTIES ("external.table.purge" = "true")
    """
    result = DDLParser(ddl, normalize_names=True).run(output_mode="hql")
    expected = [
        {
            "alter": {},
            "checks": [],
            "collection_items_terminated_by": None,
            "columns": [],
            "comment": None,
            "external": True,
            "fields_terminated_by": None,
            "if_not_exists": True,
            "index": [],
            "like": {"schema": "schema", "table_name": "table_template"},
            "lines_terminated_by": None,
            "location": '"/path/to/table"',
            "map_keys_terminated_by": None,
            "partitioned_by": [],
            "primary_key": [],
            "row_format": None,
            "schema": "schema",
            "stored_as": None,
            "table_name": "specific_table",
            "tablespace": None,
            "tblproperties": {"external.table.purge": "true"},
        }
    ]
    assert expected == result
