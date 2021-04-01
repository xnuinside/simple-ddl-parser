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
            "location": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
        },
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
            "partitioned_by": [{"name": "batch_id", "type": "int", "size": None}],
            "external": False,
            "schema": "database",
            "stored_as": None,
            "table_name": "table_name2",
            "location": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
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
            "stored_as": "PARQUET",
            "location": None,
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
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
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
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

    """

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
            "row_format": None,
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
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
            "row_format": "DELIMITED",
            "fields_terminated_by": None,
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
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
            "fields_terminated_by": ",",
            "external": False,
            "schema": "default",
            "table_name": "salesorderdetail",
            "collection_items_terminated_by": None,
            "map_keys_terminated_by": None,
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
            "collection_items_terminated_by": "'\\002'",
            "map_keys_terminated_by": None,
        }
    ]


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
            "fields_terminated_by": ",",
            "external": False,
            "schema": "default",
            "table_name": "salesorderdetail",
            "collection_items_terminated_by": "'\\002'",
            "map_keys_terminated_by": "'\\003'",
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
                    "type": "ARRAY <structcolx:string,coly:string>",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "employee_info",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "STRUCT <employer:STRING,id:BIGINT,address:STRING>",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "employee_description",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "string",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "column_abc2",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "ARRAY <structcolx:string,coly:string>",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "column_map",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "MAP <STRING,STRUCT "
                    "<year:INT,place:STRING,details:STRING>>",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "column_map_no_spaces",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "MAP <STRING,STRUCT "
                    "<year:INT,place:STRING,details:STRING>>",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "column_struct",
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "STRUCT <street_address:STRUCT "
                    "<street_number:INT,street_name:STRING,street_type:STRING>,country:STRING,postal_code:STRING>",
                    "unique": False,
                },
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
        }
    ]
    assert expected == result
