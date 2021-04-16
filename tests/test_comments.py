from simple_ddl_parser import DDLParser


def test_inline_comment():
    parse_result = DDLParser(
        """
                          drop table if exists user_history ;
    CREATE table user_history (
        runid                 decimal(21) not null
    ,job_id                decimal(21) not null
    ,id                    varchar(100) not null -- group_id or role_id
    ,user              varchar(100) not null
    ,status                varchar(10) not null
    ,event_time            timestamp not null default now()
    ,comment           varchar(1000) not null default 'none'
    ) ;
                            
                            
                            
                            """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "runid",
                    "type": "decimal",
                    "size": 21,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "job_id",
                    "type": "decimal",
                    "size": 21,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "id",
                    "type": "varchar",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "user",
                    "type": "varchar",
                    "size": 100,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "status",
                    "type": "varchar",
                    "size": 10,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "event_time",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "now()",
                    "check": None,
                },
                {
                    "name": "comment",
                    "type": "varchar",
                    "size": 1000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'none'",
                    "check": None,
                },
            ],
            "primary_key": [],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "user_history",
        }
    ]
    assert expected == parse_result


def test_block_comments():
    ddl = """
        /* outer comment start
        bla bla bla
        /* inner comment */
        select a from b

        outer comment end */
        create table A(/* 
                inner comment2 */
            data_sync_id bigint not null ,
            sync_start timestamp  not null,
            sync_end timestamp  not null,
            message varchar(2000),
            primary key (data_sync_id, sync_start, sync_end, message)
        );
        """
    parse_result = DDLParser(ddl).run()
    expected = [
        {
            "columns": [
                {
                    "name": "data_sync_id",
                    "type": "bigint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "size": 2000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["data_sync_id", "sync_start", "sync_end", "message"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "A",
        }
    ]
    assert expected == parse_result


def test_mysql_comments_support():
    ddl = """
        # this is mysql comment1
        
    /* outer comment start
    bla bla bla
    /* inner comment */
    select a from b

    outer comment end */
    create table A(/* 
            inner comment2 */
        data_sync_id bigint not null ,
        sync_start timestamp  not null,
        sync_end timestamp  not null,
    # this is mysql comment2
        message varchar(2000),
        primary key (data_sync_id, sync_start, sync_end, message)
    );
    """
    parse_result = DDLParser(ddl).run()
    expected = expected = [
        {
            "columns": [
                {
                    "name": "data_sync_id",
                    "type": "bigint",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_start",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "sync_end",
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "message",
                    "type": "varchar",
                    "size": 2000,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["data_sync_id", "sync_start", "sync_end", "message"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "partitioned_by": [],
            "table_name": "A",
        }
    ]
    assert expected == parse_result


def test_two_defices_in_string_work_ok():

    ddl = """
    CREATE TABLE "my--custom--schema"."users" (
    "id" SERIAL PRIMARY KEY,
    "name" varchar,
    "created_at" timestamp,
    "updated_at" timestamp,
    "country_code" int,
    "default_language" int
    );
    """
    parse_result = DDLParser(ddl).run()

    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "SERIAL",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"name"',
                    "type": "varchar",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"created_at"',
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"updated_at"',
                    "type": "timestamp",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"country_code"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": '"default_language"',
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ['"id"'],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": '"my--custom--schema"',
            "partitioned_by": [],
            "table_name": '"users"',
        }
    ]
    assert expected == parse_result
