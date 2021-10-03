from simple_ddl_parser import DDLParser


def test_base_encode():
    ddl = """
    create table sales(
    qtysold smallint not null encode mostly8,
    pricepaid decimal(8,2) encode delta32k,
    commission decimal(8,2) encode delta32k,
    )
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")

    expected = {
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
                        "encode": "mostly8",
                        "name": "qtysold",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "smallint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "delta32k",
                        "name": "pricepaid",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "decimal",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "delta32k",
                        "name": "commission",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "decimal",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "encode": None,
                "distkey": None,
                "diststyle": None,
                "sortkey": {"keys": [], "type": None},
                "table_name": "sales",
                "tablespace": None,
                "temp": False,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }

    assert expected == result


def test_distkey_sortkey():

    ddl = """
    create table sales(
    salesid integer not null,
    listid integer not null,
    sellerid integer not null,
    buyerid integer not null,
    eventid integer not null encode mostly16,
    dateid smallint not null,
    qtysold smallint not null encode mostly8,
    pricepaid decimal(8,2) encode delta32k,
    commission decimal(8,2) encode delta32k,
    saletime timestamp,
    primary key(salesid),
    foreign key(listid) references listing(listid),
    foreign key(sellerid) references users(userid),
    foreign key(buyerid) references users(userid),
    foreign key(dateid) references date(dateid))
    distkey(listid)
    compound sortkey(listid,sellerid);
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")
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
                        "encode": None,
                        "name": "salesid",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "listid",
                        "nullable": False,
                        "references": {
                            "column": "listid",
                            "deferrable_initially": None,
                            "on_delete": None,
                            "on_update": None,
                            "schema": None,
                            "table": "listing",
                        },
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "sellerid",
                        "nullable": False,
                        "references": {
                            "column": "userid",
                            "deferrable_initially": None,
                            "on_delete": None,
                            "on_update": None,
                            "schema": None,
                            "table": "users",
                        },
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "buyerid",
                        "nullable": False,
                        "references": {
                            "column": "userid",
                            "deferrable_initially": None,
                            "on_delete": None,
                            "on_update": None,
                            "schema": None,
                            "table": "users",
                        },
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "mostly16",
                        "name": "eventid",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "dateid",
                        "nullable": False,
                        "references": {
                            "column": "dateid",
                            "deferrable_initially": None,
                            "on_delete": None,
                            "on_update": None,
                            "schema": None,
                            "table": "date",
                        },
                        "size": None,
                        "type": "smallint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "mostly8",
                        "name": "qtysold",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "smallint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "delta32k",
                        "name": "pricepaid",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "decimal",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "delta32k",
                        "name": "commission",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "decimal",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "saletime",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "timestamp",
                        "unique": False,
                    },
                ],
                "distkey": "listid",
                "diststyle": None,
                "index": [],
                "partitioned_by": [],
                "primary_key": ["salesid"],
                "schema": None,
                "encode": None,
                "sortkey": {"keys": ["listid", "sellerid"], "type": "compound"},
                "table_name": "sales",
                "tablespace": None,
                "temp": False,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_distyle():

    ddl = """
    create table t1(col1 int distkey) diststyle key;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")

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
                        "encode": None,
                        "name": "col1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    }
                ],
                "distkey": "col1",
                "diststyle": "KEY",
                "index": [],
                "encode": None,
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "sortkey": {"keys": [], "type": None},
                "table_name": "t1",
                "tablespace": None,
                "temp": False,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_encode_for_full_table():

    ddl = """
    create table t2(c0 int, c1 varchar) encode auto;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")
    expected = {
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
                        "encode": "auto",
                        "name": "c0",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "auto",
                        "name": "c1",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "varchar",
                        "unique": False,
                    },
                ],
                "distkey": None,
                "diststyle": None,
                "encode": "auto",
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "sortkey": {"keys": [], "type": None},
                "table_name": "t2",
                "tablespace": None,
                "temp": False,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_interleaved_sortkey_also_ok():

    ddl = """
    create table customer_interleaved (
    c_custkey     	integer        not null,
    c_name        	varchar(25)    not null,
    c_address     	varchar(25)    not null,
    c_city        	varchar(10)    not null,
    c_nation      	varchar(15)    not null,
    c_region      	varchar(12)    not null,
    c_phone       	varchar(15)    not null,
    c_mktsegment      varchar(10)    not null)
    diststyle all
    interleaved sortkey (c_custkey, c_city, c_mktsegment);
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")
    expected = {
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
                        "encode": None,
                        "name": "c_custkey",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_name",
                        "nullable": False,
                        "references": None,
                        "size": 25,
                        "type": "varchar",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_address",
                        "nullable": False,
                        "references": None,
                        "size": 25,
                        "type": "varchar",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_city",
                        "nullable": False,
                        "references": None,
                        "size": 10,
                        "type": "varchar",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_nation",
                        "nullable": False,
                        "references": None,
                        "size": 15,
                        "type": "varchar",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_region",
                        "nullable": False,
                        "references": None,
                        "size": 12,
                        "type": "varchar",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_phone",
                        "nullable": False,
                        "references": None,
                        "size": 15,
                        "type": "varchar",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": None,
                        "name": "c_mktsegment",
                        "nullable": False,
                        "references": None,
                        "size": 10,
                        "type": "varchar",
                        "unique": False,
                    },
                ],
                "distkey": None,
                "diststyle": "all",
                "encode": None,
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "sortkey": {
                    "keys": ["c_custkey", "c_city", "c_mktsegment"],
                    "type": "interleaved",
                },
                "table_name": "customer_interleaved",
                "tablespace": None,
                "temp": False,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_create_temp_table():

    ddl = """
    create temp table tempevent(
        qtysold smallint not null encode mostly8,
        pricepaid decimal(8,2) encode delta32k,
        commission decimal(8,2) encode delta32k,
        );
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")
    expected = {
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
                        "encode": "mostly8",
                        "name": "qtysold",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "smallint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "delta32k",
                        "name": "pricepaid",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "decimal",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encode": "delta32k",
                        "name": "commission",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "decimal",
                        "unique": False,
                    },
                ],
                "distkey": None,
                "diststyle": None,
                "encode": None,
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "sortkey": {"keys": [], "type": None},
                "table_name": "tempevent",
                "tablespace": None,
                "temp": True,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result

    ddl = """
    create temporary table tempevent(
        qtysold smallint not null encode mostly8,
        pricepaid decimal(8,2) encode delta32k,
        commission decimal(8,2) encode delta32k,
        );
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")
    assert expected == result


def test_like_in_parath():
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [],
                "distkey": None,
                "diststyle": None,
                "encode": None,
                "index": [],
                "like": {"schema": None, "table_name": "event"},
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "sortkey": {"keys": [], "type": None},
                "table_name": "tempevent",
                "tablespace": None,
                "temp": True,
            }
        ],
        "types": [],
    }
    ddl = """
    create temp table tempevent(like event);
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="redshift")
    assert expected == result
