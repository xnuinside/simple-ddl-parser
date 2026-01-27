"""Custom output schema example for DDLParser."""

from simple_ddl_parser import DDLParser, register_custom_output_schema


def to_custom_schema(output):
    # Example: return a minimal summary structure.
    return [{"tables": len(output), "custom": True}]


def main() -> None:
    ddl = """
    CREATE TABLE users (
        id INT NOT NULL,
        email VARCHAR(255)
    );
    """

    register_custom_output_schema("summary", to_custom_schema)
    result = DDLParser(ddl).run(custom_output_schema="summary")
    print(result)


if __name__ == "__main__":
    main()
