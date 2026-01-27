from simple_ddl_parser.ddl_parser import (
    DDLParser,
    DDLParserError,
    SimpleDDLParserException,
    parse_from_file,
)
from simple_ddl_parser.output.dialects import dialect_by_name
from simple_ddl_parser.output.custom_schemas import (
    list_custom_output_schemas,
    register_custom_output_schema,
)

supported_dialects = dialect_by_name

__all__ = [
    "DDLParser",
    "parse_from_file",
    "DDLParserError",
    "supported_dialects",
    "SimpleDDLParserException",
    "register_custom_output_schema",
    "list_custom_output_schemas",
]
