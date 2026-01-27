import re
from typing import Callable, Dict, Iterable, List, Optional, Union

from simple_ddl_parser.exception import SimpleDDLParserException

OutputPayload = Union[List[Dict], Dict]
CustomSchemaFunc = Callable[[OutputPayload], List[Dict]]

_CUSTOM_OUTPUT_SCHEMAS: Dict[str, CustomSchemaFunc] = {}


def register_custom_output_schema(name: str, handler: CustomSchemaFunc) -> None:
    if not name or not isinstance(name, str):
        raise SimpleDDLParserException("Custom output schema name must be a non-empty string.")
    if not callable(handler):
        raise SimpleDDLParserException("Custom output schema handler must be callable.")
    _CUSTOM_OUTPUT_SCHEMAS[name.lower()] = handler


def unregister_custom_output_schema(name: str) -> None:
    _CUSTOM_OUTPUT_SCHEMAS.pop(name.lower(), None)


def list_custom_output_schemas() -> List[str]:
    return sorted(_CUSTOM_OUTPUT_SCHEMAS.keys())


def apply_custom_output_schema(
    custom_output_schema: Union[str, CustomSchemaFunc], output: OutputPayload
) -> List[Dict]:
    if callable(custom_output_schema):
        return custom_output_schema(output)
    schema_name = custom_output_schema.lower()
    handler = _CUSTOM_OUTPUT_SCHEMAS.get(schema_name)
    if handler is None:
        supported = ", ".join(list_custom_output_schemas())
        raise SimpleDDLParserException(
            "Custom output schema can be one of the following: "
            f"[{supported}]"
        )
    return handler(output)


_BQ_TYPE_MAP = {
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "BIGINT": "INTEGER",
    "SMALLINT": "INTEGER",
    "TINYINT": "INTEGER",
    "INT64": "INTEGER",
    "FLOAT": "FLOAT",
    "FLOAT64": "FLOAT",
    "DOUBLE": "FLOAT",
    "REAL": "FLOAT",
    "NUMERIC": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "CHAR": "STRING",
    "NCHAR": "STRING",
    "VARCHAR": "STRING",
    "NVARCHAR": "STRING",
    "STRING": "STRING",
    "TEXT": "STRING",
    "BOOL": "BOOL",
    "BOOLEAN": "BOOL",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "JSON": "JSON",
    "GEOGRAPHY": "GEOGRAPHY",
    "BYTES": "BYTES",
}

_ARRAY_RE = re.compile(r"^ARRAY<(.+)>$", re.IGNORECASE)
_STRUCT_RE = re.compile(r"^STRUCT<(.+)>$", re.IGNORECASE)


def _normalize_type(type_value: Optional[str]) -> str:
    if not type_value:
        return "STRING"
    base = type_value.strip().upper()
    if "(" in base:
        base = base.split("(", 1)[0].strip()
    return re.sub(r"\s+", " ", base)


def _extract_tables(output: OutputPayload) -> Iterable[Dict]:
    if isinstance(output, dict):
        return output.get("tables", [])
    return [item for item in output if isinstance(item, dict) and "table_name" in item]


def _to_bigquery_schema(output: OutputPayload) -> List[Dict]:
    result = []
    for table in _extract_tables(output):
        fields = []
        for column in table.get("columns", []):
            raw_type = column.get("type")
            mode = "REQUIRED" if column.get("nullable") is False else "NULLABLE"
            normalized = _normalize_type(raw_type)
            array_match = _ARRAY_RE.match(normalized)
            struct_match = _STRUCT_RE.match(normalized)
            if array_match:
                normalized = _normalize_type(array_match.group(1))
                mode = "REPEATED"
            elif struct_match:
                normalized = "RECORD"
            field_type = _BQ_TYPE_MAP.get(normalized, normalized)
            fields.append({"name": column.get("name"), "type": field_type, "mode": mode})

        entry = {"table_name": table.get("table_name"), "schema": fields}
        if table.get("schema"):
            entry["schema_name"] = table.get("schema")
        if table.get("dataset"):
            entry["dataset"] = table.get("dataset")
        if table.get("project"):
            entry["project"] = table.get("project")
        result.append(entry)
    return result


register_custom_output_schema("bigquery", _to_bigquery_schema)
