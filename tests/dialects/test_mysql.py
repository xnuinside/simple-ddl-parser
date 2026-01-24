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


def test_character_set_table_option():
    ddl = """
    CREATE TABLE `tab_space_station_common_info`  (
      `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
      `col1` varchar(16) NULL DEFAULT NULL,
      PRIMARY KEY (`id`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8mb4;
    """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="mysql")
    assert len(result["tables"]) == 1
    assert result["tables"][0]["default_charset"] == "utf8mb4"


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
            "table_properties": {"collate": "utf8mb4_unicode_ci"},
        }
    ]
    assert result == expected


def test_enum_column_type():
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
                        "default": "'enabled'",
                        "name": "cancellation_type",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "ENUM",
                        "unique": False,
                        "values": ["'enabled'", "'disabled'"],
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "myset",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    ddl = """
CREATE TABLE myset (
     cancellation_type enum('enabled','disabled') NOT NULL DEFAULT 'enabled'
);
"""
    result = DDLParser(ddl, debug=True).run(
        group_by_type=True,
        output_mode="mysql",
    )
    assert result == expected


def test_set_type():
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
                        "name": "randomcolumn",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "SET",
                        "unique": False,
                        "values": ["'a'", "'b'", "'c'", "'d'"],
                    }
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "myset",
                "tablespace": None,
            }
        ],
        "types": [],
    }

    ddl = """
    CREATE TABLE myset (
        randomcolumn SET('a', 'b', 'c', 'd')
    );
    """
    result = DDLParser(ddl, debug=True).run(
        group_by_type=True,
        output_mode="mysql",
    )
    assert expected == result


def test_mysql_character_set():
    ddl = """
    CREATE TABLE `table_notes` (
    `id` int NOT NULL AUTO_INCREMENT,
    `notes` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    );"""
    result = DDLParser(ddl, debug=True).run(
        group_by_type=True,
        output_mode="mysql",
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
                "columns": [
                    {
                        "autoincrement": True,
                        "check": None,
                        "default": None,
                        "name": "`id`",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    },
                    {
                        "character_set": "utf8mb3",
                        "check": None,
                        "collate": "utf8mb3_general_ci",
                        "default": None,
                        "name": "`notes`",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "varchar",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "`table_notes`",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_unicode_right_single_quote_in_comment():
    """Test for issue #297: Unicode right single quotation mark (U+2019) in COMMENT.

    The parser should handle Unicode curly quotes inside string literals without
    raising DDLParserError.
    https://github.com/xnuinside/simple-ddl-parser/issues/297
    """
    # U+2019 is RIGHT SINGLE QUOTATION MARK (')
    # Using exact DDL from issue #297
    ddl = """CREATE TABLE `example_table` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL COMMENT 'double width single quote ’ in comment',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""

    result = DDLParser(ddl).run(output_mode="mysql")

    assert len(result) == 1
    assert result[0]["table_name"] == "`example_table`"
    assert len(result[0]["columns"]) == 2

    # Check id column
    id_col = result[0]["columns"][0]
    assert id_col["name"] == "`id`"
    assert id_col["type"] == "INT"
    assert id_col["autoincrement"] is True

    # Check name column with unicode quote in comment
    name_col = result[0]["columns"][1]
    assert name_col["name"] == "`name`"
    assert name_col["type"] == "VARCHAR"
    assert name_col["size"] == 255
    assert name_col["nullable"] is False
    # The comment should contain the escaped unicode character
    assert name_col["comment"] == "'double width single quote \\u2019 in comment'"

    # Check table properties
    assert result[0]["primary_key"] == ["`id`"]
    assert result[0]["engine"] == "InnoDB"
    assert result[0]["default_charset"] == "utf8mb4"


def test_unicode_left_single_quote_in_comment():
    """Test Unicode left single quotation mark (U+2018) in COMMENT."""
    # U+2018 is LEFT SINGLE QUOTATION MARK (‘)
    ddl = """CREATE TABLE t (`col` VARCHAR(100) COMMENT 'value with ‘ quote');"""

    result = DDLParser(ddl).run(output_mode="mysql")

    assert len(result) == 1
    assert result[0]["columns"][0]["name"] == "`col`"
    assert "\\u2018" in result[0]["columns"][0]["comment"]


def test_unicode_both_curly_quotes_in_comment():
    """Test both Unicode curly quotes (U+2018 and U+2019) in COMMENT."""
    # Using both left (‘) and right (’) single quotation marks
    ddl = """CREATE TABLE t (`col` VARCHAR(100) COMMENT 'text ‘quoted’ here');"""

    result = DDLParser(ddl).run(output_mode="mysql")

    assert len(result) == 1
    comment = result[0]["columns"][0]["comment"]
    assert "\\u2018" in comment
    assert "\\u2019" in comment


def test_unicode_quotes_in_column_default():
    """Test Unicode curly quotes in DEFAULT value."""
    ddl = """CREATE TABLE t (`col` VARCHAR(100) DEFAULT 'it’s a test');"""

    result = DDLParser(ddl).run(output_mode="mysql")

    assert len(result) == 1
    default_val = result[0]["columns"][0]["default"]
    assert "\\u2019" in default_val
