from simple_ddl_parser.ddl_parser import DDLParser, DDLParserError, parse_from_file
from simple_ddl_parser.output.dialects import dialect_by_name

supported_dialects = dialect_by_name

__all__ = ["DDLParser", "parse_from_file", "DDLParserError", "supported_dialects"]
