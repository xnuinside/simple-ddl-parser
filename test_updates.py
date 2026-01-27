"""Small helper script to run the parser against a sample DDL."""

from simple_ddl_parser import DDLParser


def main() -> None:
    ddl = """
    CREATE TABLE users (
        id BIGINT NOT NULL,
        email VARCHAR(255),
        created_at TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE
    );
    """

    parser = DDLParser(ddl)
    result = parser.run()
    print(result)


if __name__ == "__main__":
    main()

# Example proposal (not implemented yet) for a BigQuery schema helper:
# result = DDLParser(ddl).run()
# bq_schema = DDLParser(ddl).to_bigquery_schema(table="users")
# print(bq_schema)
