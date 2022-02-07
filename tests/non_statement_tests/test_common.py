import pytest

from simple_ddl_parser import DDLParser, DDLParserError


def test_no_unexpected_logs(capsys):

    ddl = """
    CREATE EXTERNAL TABLE test (
    test STRING NULL COMMENT 'xxxx',
    )
    PARTITIONED BY (snapshot STRING, cluster STRING)
    """

    parser = DDLParser(ddl)
    out, err = capsys.readouterr()
    assert out == ""
    assert err == ""
    parser.run(output_mode="hql", group_by_type=True)
    out, err = capsys.readouterr()
    assert out == ""
    assert err == ""


def test_silent_false_flag():
    ddl = """
CREATE TABLE foo
        (
  created_timestamp  TIMESTAMPTZ  NOT NULL DEFAULT ALTER (now() at time zone 'utc')
        );
"""
    with pytest.raises(DDLParserError) as e:
        DDLParser(ddl, silent=False).run(group_by_type=True)

        assert "Unknown statement" in e.value[1]


def test_flag_normalize_names():

    ddl = ddl = """/****** Object:  Table [dbo].[TO_Requests]    Script Date: 9/29/2021 9:55:26 PM ******/
    SET ANSI_NULLS ON
    GO
    SET QUOTED_IDENTIFIER ON
    GO
    CREATE TABLE [dbo].[TO_Requests](
        [Request_ID] [int] IDENTITY(1,1) NOT NULL,
        [user_id] [int] NULL,
        [date_from] [smalldatetime] NULL,)"""
    result = DDLParser(ddl, silent=False, normalize_names=True).run(group_by_type=True)
    expected = {
        "comments": [
            "***** Object:  Table [dbo].[TO_Requests]    Script Date: "
            "9/29/2021 9:55:26 PM ******/"
        ],
        "ddl_properties": [
            {"name": "ANSI_NULLS", "value": "ON"},
            {"name": "QUOTED_IDENTIFIER", "value": "ON"},
        ],
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
                        "name": "Request_ID",
                        "nullable": False,
                        "references": None,
                        "size": (1, 1),
                        "type": "int IDENTITY",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "user_id",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "int",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "date_from",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "smalldatetime",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": "dbo",
                "table_name": "TO_Requests",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result
