from simple_ddl_parser import DDLParser

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
    result = parser.run(output_mode="hql", group_by_type=True)
    out, err = capsys.readouterr()
    assert out == ""
    assert err == ""