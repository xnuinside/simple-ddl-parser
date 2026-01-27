"""BigQuery custom output schema example for DDLParser."""

from simple_ddl_parser import DDLParser


def main() -> None:
    ddl = """
    CREATE TABLE project_id.dataset.users (
        id INT64 NOT NULL,
        tags ARRAY<STRING>,
        meta STRUCT<a ARRAY<STRING>, b BOOL>
    );
    """

    result = DDLParser(ddl).run(custom_output_schema="bigquery")
    print(result)


if __name__ == "__main__":
    main()
