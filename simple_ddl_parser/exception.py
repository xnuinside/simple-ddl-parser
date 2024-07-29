__all__ = [
    "SimpleDDLParserException",
]


class SimpleDDLParserException(Exception):
    """ Base exception in simple ddl parser library """
    pass


class DDLParserError(SimpleDDLParserException):
    """ An alias for backward compatibility """
    pass
