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
