import os

from simple_ddl_parser import parse_from_file


def test_parse_from_file_one_table():
    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "SERIAL",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"name"',
                    "type": "varchar",
                    "size": 160,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"created_at"',
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"updated_at"',
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"country_code"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"default_language"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ['"id"'],
            "index": [],
            "table_name": '"users"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
            "alter": {},
            "checks": [],
        }
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(
        os.path.join(current_path, "sql", "test_one_table.sql")
    )


def test_parse_from_file_mysql_named_foreign_key_with_silent_false(tmp_path):
    ddl_file = tmp_path / "named_fk.sql"
    ddl_file.write_text(
        """
        CREATE TABLE parent (
            id int PRIMARY KEY
        );

        CREATE TABLE child (
            parent_id int
        );

        ALTER TABLE child ADD CONSTRAINT fk_child_parent FOREIGN KEY fk_child_parent (parent_id) REFERENCES parent (id);
        """,
        encoding="utf-8",
    )

    result = parse_from_file(
        str(ddl_file),
        parser_settings={"silent": False},
        output_mode="mysql",
    )

    assert result[1]["table_name"] == "child"
    assert result[1]["alter"]["columns"][0]["constraint_name"] == "fk_child_parent"


def test_parse_from_file_two_statements():
    expected = [
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "SERIAL",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"name"',
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"created_at"',
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"updated_at"',
                    "type": "timestamp",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"country_code"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"default_language"',
                    "type": "int",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ['"id"'],
            "index": [],
            "table_name": '"users"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
            "alter": {},
            "checks": [],
        },
        {
            "columns": [
                {
                    "name": '"id"',
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"code"',
                    "type": "varchar",
                    "size": 2,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
                {
                    "name": '"name"',
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "unique": False,
                    "references": None,
                },
            ],
            "primary_key": ['"id"'],
            "index": [],
            "table_name": '"languages"',
            "tablespace": None,
            "schema": None,
            "partitioned_by": [],
            "alter": {},
            "checks": [],
        },
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(
        os.path.join(current_path, "sql", "test_two_tables.sql")
    )


def test_parse_from_file_encoding():
    expected = [
        {
            "columns": [],
            "primary_key": [],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "tablespace": None,
            "if_exists": True,
            "schema": None,
            "table_name": "`mangos_string`",
        },
        {
            "columns": [
                {
                    "name": "`entry`",
                    "type": "mediumint unsigned",
                    "size": 8,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": "'0'",
                    "check": None,
                },
                {
                    "name": "`content_default`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc1`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc2`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc3`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc4`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc5`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc6`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc7`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "`content_loc8`",
                    "type": "text",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": ["`entry`"],
            "alter": {},
            "checks": [],
            "index": [],
            "partitioned_by": [],
            "tablespace": None,
            "if_not_exists": True,
            "schema": None,
            "table_name": "`mangos_string`",
        },
        {
            "comments": [
                "!40101 SET @OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT ",
                "!40101 SET NAMES utf8 ",
                "!40014 SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS = 0 ",
                "!40101 SET @OLD_SQL_MODE = @@SQL_MODE, SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO' ",
                "!40000 ALTER TABLE `mangos_string` DISABLE KEYS ",
                "!40000 ALTER TABLE `mangos_string` ENABLE KEYS ",
                "!40101 SET SQL_MODE = IFNULL(@OLD_SQL_MODE, '') ",
                "!40014 SET FOREIGN_KEY_CHECKS = IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, "
                "@OLD_FOREIGN_KEY_CHECKS) ",
                "!40101 SET CHARACTER_SET_CLIENT = @OLD_CHARACTER_SET_CLIENT ",
            ]
        },
    ]

    current_path = os.path.dirname(os.path.abspath(__file__))
    result = parse_from_file(
        os.path.join(current_path, "sql", "mangos_encoding_test.sql")
    )
    assert expected == result


def test_parse_from_file_issue_148_drop_table_if_exists_and_commit(tmp_path):
    ddl_file = tmp_path / "issue_148_quartz.sql"
    ddl_file.write_text(
        """
        DROP TABLE IF EXISTS QRTZ_JOB_DETAILS;

        CREATE TABLE IF NOT EXISTS QRTZ_JOB_DETAILS (
            SCHED_NAME VARCHAR(120) NOT NULL,
            JOB_NAME VARCHAR(200) NOT NULL,
            PRIMARY KEY (SCHED_NAME, JOB_NAME)
        ) ENGINE=InnoDB;

        COMMIT;
        """,
        encoding="utf-8",
    )

    result = parse_from_file(
        str(ddl_file),
        parser_settings={"silent": False},
        output_mode="mysql",
    )

    assert len(result) == 2
    assert result[0]["table_name"] == "QRTZ_JOB_DETAILS"
    assert result[0]["if_exists"] is True
    assert result[0]["columns"] == []
    assert result[1]["table_name"] == "QRTZ_JOB_DETAILS"
    assert result[1]["if_not_exists"] is True


def test_parse_from_file_issue_148_mediawiki_comments_and_mysql_indexes(tmp_path):
    ddl_file = tmp_path / "issue_148_mediawiki.sql"
    ddl_file.write_text(
        """
        -- Only the 'searchindex' table requires MyISAM.
        -- The installer injects a prefix through the comment below.
        CREATE TABLE /*$wgDBprefix*/category (
          cat_id int unsigned NOT NULL auto_increment,
          cat_title varchar(255) binary NOT NULL,
          cat_pages int signed NOT NULL default 0,
          PRIMARY KEY (cat_id),
          UNIQUE KEY (cat_title),
          KEY (cat_pages),
          KEY idx_category_title (cat_title(32))
        ) /*$wgDBTableOptions*/;
        """,
        encoding="utf-8",
    )

    result = parse_from_file(
        str(ddl_file),
        parser_settings={"silent": False},
        output_mode="mysql",
    )

    assert result[0]["table_name"] == "category"
    assert result[0]["primary_key"] == ["cat_id"]
    assert result[0]["columns"][1]["unique"] is True
    assert result[0]["index"][0]["columns"] == ["cat_pages"]
    assert result[0]["index"][1]["detailed_columns"][0]["length"] == 32


def test_parse_from_file_issue_147_inline_foreign_key_on_delete_set_null(tmp_path):
    ddl_file = tmp_path / "issue_147_comtccmmncode.sql"
    ddl_file.write_text(
        """
        CREATE TABLE COMTCCMMNCLCODE
        (
            CL_CODE               CHAR(3)  NOT NULL,
            CL_CODE_NM            VARCHAR2(60)  NULL,
            CONSTRAINT COMTCCMMNCLCODE_PK PRIMARY KEY (CL_CODE)
        );

        CREATE TABLE COMTCCMMNCODE
        (
            CODE_ID               VARCHAR2(6)  NOT NULL,
            CODE_ID_NM            VARCHAR2(60)  NULL,
            CODE_ID_DC            VARCHAR2(200)  NULL,
            USE_AT                CHAR(1)  NULL,
            CL_CODE               CHAR(3)  NULL,
            FRST_REGIST_PNTTM     DATE  NULL,
            FRST_REGISTER_ID      VARCHAR2(20)  NULL,
            LAST_UPDT_PNTTM       DATE  NULL,
            LAST_UPDUSR_ID        VARCHAR2(20)  NULL,
            CONSTRAINT COMTCCMMNCODE_PK PRIMARY KEY (CODE_ID),
            CONSTRAINT COMTCCMMNCODE_FK1 FOREIGN KEY (CL_CODE)
                REFERENCES COMTCCMMNCLCODE(CL_CODE) ON DELETE SET NULL
        );
        """,
        encoding="utf-8",
    )

    result = parse_from_file(str(ddl_file), parser_settings={"silent": False})

    assert len(result) == 2
    assert result[1]["table_name"] == "COMTCCMMNCODE"
    assert result[1]["columns"][4]["name"] == "CL_CODE"
    assert result[1]["columns"][4]["references"] == {
        "table": "COMTCCMMNCLCODE",
        "schema": None,
        "on_delete": "SET NULL",
        "on_update": None,
        "deferrable_initially": None,
        "constraint_name": "COMTCCMMNCODE_FK1",
        "column": "CL_CODE",
    }
