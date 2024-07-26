from simple_ddl_parser import DDLParser


def test_simple_on_update():
    ddl = """CREATE TABLE t1 (
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"""
    result = DDLParser(ddl).run(group_by_type=True, output_mode="mysql")
    expected = {
        "tables": [
            {
                "columns": [
                    {
                        "name": "ts",
                        "type": "TIMESTAMP",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": "CURRENT_TIMESTAMP",
                        "check": None,
                        "on_update": "CURRENT_TIMESTAMP",
                    },
                    {
                        "name": "dt",
                        "type": "DATETIME",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": "CURRENT_TIMESTAMP",
                        "check": None,
                        "on_update": "CURRENT_TIMESTAMP",
                    },
                ],
                "primary_key": [],
                "alter": {},
                "checks": [],
                "index": [],
                "partitioned_by": [],
                "tablespace": None,
                "schema": None,
                "table_name": "t1",
            }
        ],
        "types": [],
        "sequences": [],
        "domains": [],
        "schemas": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_on_update_with_fcall():
    ddl = """create table test(
    `id` bigint not null,
    `updated_at` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),
    primary key (id));"""
    result = DDLParser(ddl).run(group_by_type=True, output_mode="mysql")
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
                        "name": "`id`",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "bigint",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "current_timestamp(3)",
                        "name": "`updated_at`",
                        "nullable": False,
                        "on_update": "current_timestamp(3)",
                        "references": None,
                        "size": 3,
                        "type": "timestamp",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": ["id"],
                "schema": None,
                "table_name": "test",
                "tablespace": None,
            }
        ],
        "ddl_properties": [],
        "types": [],
    }
    assert expected == result


def test_default_charset():
    results = DDLParser(
        """
    CREATE TABLE t_table_records (
    id VARCHAR (255) NOT NULL,
    create_time datetime DEFAULT CURRENT_TIMESTAMP NOT NULL,
    creator VARCHAR (32) DEFAULT 'sys' NOT NULL,
    current_rows BIGINT,
    edit_time datetime DEFAULT CURRENT_TIMESTAMP NOT NULL,
    editor VARCHAR (32) DEFAULT 'sys' NOT NULL,
    managed_database_database VARCHAR (255) NOT NULL,
    managed_database_schema VARCHAR (255),
    managed_database_table VARCHAR (255) NOT NULL,
    source_database_database VARCHAR (255) NOT NULL,
    source_database_jdbc VARCHAR (255) NOT NULL,
    source_database_schema VARCHAR (255),
    source_database_table VARCHAR (255) NOT NULL,
    source_database_type VARCHAR (255) NOT NULL,
    source_rows BIGINT,
    PRIMARY KEY (id)
    ) ENGINE = INNODB DEFAULT CHARSET = utf8mb4 COMMENT = '导入元数据管理';
    """
    ).run(group_by_type=True, output_mode="mysql")

    expected = {
        "ddl_properties": [],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "engine": "INNODB",
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "id",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "CURRENT_TIMESTAMP",
                        "name": "create_time",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "datetime",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "'sys'",
                        "name": "creator",
                        "nullable": False,
                        "references": None,
                        "size": 32,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "current_rows",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "BIGINT",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "CURRENT_TIMESTAMP",
                        "name": "edit_time",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "datetime",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": "'sys'",
                        "name": "editor",
                        "nullable": False,
                        "references": None,
                        "size": 32,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "managed_database_database",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "managed_database_schema",
                        "nullable": True,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "managed_database_table",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "source_database_database",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "source_database_jdbc",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "source_database_schema",
                        "nullable": True,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "source_database_table",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "source_database_type",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "VARCHAR",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "source_rows",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "BIGINT",
                        "unique": False,
                    },
                ],
                "comment": "'\\u5bfc\\u5165\\u5143\\u6570\\u636e\\u7ba1\\u7406'",
                "default_charset": "utf8mb4",
                "index": [],
                "partitioned_by": [],
                "primary_key": ["id"],
                "schema": None,
                "table_name": "t_table_records",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == results


def test_identity_with_properties():
    ddl = """
CREATE TABLE IF NOT EXISTS database.table_name
    (
        [cifno] [numeric](10, 0) IDENTITY(1,1) NOT NULL,
    )
"""

    result = DDLParser(ddl).run(output_mode="mysql")
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "identity": (1, 1),
                    "name": "[cifno]",
                    "nullable": False,
                    "references": None,
                    "size": (10, 0),
                    "type": "[numeric]",
                    "unique": False,
                }
            ],
            "if_not_exists": True,
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": "database",
            "table_name": "table_name",
            "tablespace": None,
        }
    ]
    assert expected == result


def test_visible():
    ddl = """CREATE TABLE IF NOT EXISTS `ohs`.`authorized_users` (
      `id` INT(6) UNSIGNED NOT NULL AUTO_INCREMENT,
      `signum` VARCHAR(256) NOT NULL,
      `role` INT(2) UNSIGNED NOT NULL,
      `first_name` VARCHAR(64) NOT NULL,
      `last_name` VARCHAR(64) NOT NULL,
      `created_at` DATETIME NULL DEFAULT NULL,
      `created_by` VARCHAR(128) NOT NULL,
      `updated_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
      `updated_by` VARCHAR(128) NULL DEFAULT NULL,
      PRIMARY KEY (`id`),
      INDEX `id` (`id` ASC) VISIBLE)
    ENGINE = InnoDB"""

    result = DDLParser(ddl).run(output_mode="snowflake")
    expected = [
        {
            "alter": {},
            "checks": [],
            "clone": None,
            "columns": [
                {
                    "autoincrement": True,
                    "check": None,
                    "default": None,
                    "name": "`id`",
                    "nullable": False,
                    "references": None,
                    "size": 6,
                    "type": "INT UNSIGNED",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "`signum`",
                    "nullable": False,
                    "references": None,
                    "size": 256,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "`role`",
                    "nullable": False,
                    "references": None,
                    "size": 2,
                    "type": "INT UNSIGNED",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "`first_name`",
                    "nullable": False,
                    "references": None,
                    "size": 64,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "`last_name`",
                    "nullable": False,
                    "references": None,
                    "size": 64,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": "NULL",
                    "name": "`created_at`",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "DATETIME",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "`created_by`",
                    "nullable": False,
                    "references": None,
                    "size": 128,
                    "type": "VARCHAR",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": "CURRENT_TIMESTAMP",
                    "name": "`updated_at`",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "TIMESTAMP",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": "NULL",
                    "name": "`updated_by`",
                    "nullable": True,
                    "references": None,
                    "size": 128,
                    "type": "VARCHAR",
                    "unique": False,
                },
            ],
            "external": False,
            "if_not_exists": True,
            "index": [
                {
                    "clustered": False,
                    "columns": [[{"name": "`id`", "nulls": "LAST", "order": "ASC"}]],
                    "detailed_columns": [
                        {
                            "name": [{"name": "`id`", "nulls": "LAST", "order": "ASC"}],
                            "nulls": "LAST",
                            "order": "ASC",
                        }
                    ],
                    "index_name": "`id`",
                    "unique": False,
                    "visible": True,
                }
            ],
            "partitioned_by": [],
            "primary_key": ["`id`"],
            "primary_key_enforced": None,
            "schema": "`ohs`",
            "table_name": "`authorized_users`",
            "table_properties": {"engine": "InnoDB"},
            "tablespace": None,
        }
    ]
    assert result == expected


def test_auto_increment_table_property():
    expected = [
        {
            "alter": {},
            "auto_increment": "10",
            "checks": [],
            "columns": [
                {
                    "autoincrement": True,
                    "check": None,
                    "default": None,
                    "name": "`user_id`",
                    "nullable": False,
                    "references": None,
                    "size": 11,
                    "type": "int",
                    "unique": False,
                },
                {
                    "check": None,
                    "default": None,
                    "name": "`user_name`",
                    "nullable": False,
                    "references": None,
                    "size": 50,
                    "type": "varchar",
                    "unique": False,
                },
                {
                    "check": None,
                    "comment": "'user auth'",
                    "default": "'1'",
                    "name": "`authority`",
                    "nullable": True,
                    "references": None,
                    "size": 11,
                    "type": "int",
                    "unique": False,
                },
            ],
            "default_charset": "utf8",
            "engine": "InnoDB",
            "index": [
                {
                    "clustered": False,
                    "columns": ["`user_id`"],
                    "detailed_columns": [
                        {"name": "`user_id`", "nulls": "LAST", "order": "ASC"}
                    ],
                    "index_name": "`FK_authority`",
                    "unique": False,
                }
            ],
            "partitioned_by": [],
            "primary_key": ["`user_id`"],
            "schema": None,
            "table_name": "`employee`",
            "tablespace": None,
        }
    ]

    ddl = """CREATE TABLE `employee` (
      `user_id` int(11) NOT NULL AUTO_INCREMENT,
      `user_name` varchar(50) NOT NULL,
      `authority` int(11) DEFAULT '1' COMMENT 'user auth',
      PRIMARY KEY (`user_id`),
      KEY `FK_authority` (`user_id`,`user_name`)
    ) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;"""

    result = DDLParser(ddl).run(output_mode="mysql")
    assert result == expected


def test_column_index():
    ddl = """CREATE TABLE `posts`(
        `integer_column__index` INT NOT NULL INDEX
    );"""

    result = DDLParser(ddl).run()
    expected = [
        {
            "alter": {},
            "checks": [],
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "index": True,
                    "name": "`integer_column__index`",
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "INT",
                    "unique": False,
                }
            ],
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": None,
            "table_name": "`posts`",
            "tablespace": None,
        }
    ]

    assert result == expected

def test_table_properties():
    ddl = """CREATE TABLE `posts`(
        `integer_column__index` INT NOT NULL INDEX
    ) ENGINE=InnoDB AUTO_INCREMENT=4682 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='test';"""

    result = DDLParser(ddl).run(output_mode="mysql")
    expected = [
        {
            "alter": {},
            "checks": [],
            "auto_increment": "4682",
            "columns": [
                {
                    "check": None,
                    "default": None,
                    "index": True,
                    "name": "`integer_column__index`",
                    "nullable": False,
                    "references": None,
                    "size": None,
                    "type": "INT",
                    "unique": False,
                }
            ],
            "comment": "'test'",
            "default_charset": "utf8mb4",
            "engine": "InnoDB",
            "index": [],
            "partitioned_by": [],
            "primary_key": [],
            "schema": None,
            "table_name": "`posts`",
            "tablespace": None,
            "table_properties": {"collate": "utf8mb4_unicode_ci"}
        }
    ]
    assert result == expected