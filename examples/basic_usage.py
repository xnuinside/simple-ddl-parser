"""Basic usage example for DDLParser."""

from simple_ddl_parser import DDLParser


def main() -> None:
    ddl = """
    CREATE TABLE users (
        id BIGINT NOT NULL,
        email VARCHAR(255),
        created_at TIMESTAMP
    );
    """

    result = DDLParser(ddl).run()
    print(result)


if __name__ == "__main__":
    main()
