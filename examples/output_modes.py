"""Output mode examples and how they affect the result."""

from simple_ddl_parser import DDLParser


def main() -> None:
    ddl = """
    CREATE TABLE mydataset.newtable (
        id INT64 NOT NULL
    )
    PARTITION BY DATE(_PARTITIONTIME);
    """

    # Default mode returns common SQL fields only.
    sql_output = DDLParser(ddl).run(output_mode="sql")

    # BigQuery mode adds BigQuery-specific fields like dataset/project/partition_by.
    bigquery_output = DDLParser(ddl).run(output_mode="bigquery")

    print(sql_output)
    print(bigquery_output)


if __name__ == "__main__":
    main()
