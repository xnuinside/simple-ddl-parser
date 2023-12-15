# statements that used at the start of definition or in statements without columns
definition_statements = {
    "DROP": "DROP",
    "CREATE": "CREATE",
    "TABLE": "TABLE",
    "DATABASE": "DATABASE",
    "SCHEMA": "SCHEMA",
    "ALTER": "ALTER",
    "TYPE": "TYPE",
    "DOMAIN": "DOMAIN",
    "REPLACE": "REPLACE",
    "OR": "OR",
    "CLUSTERED": "CLUSTERED",
    "SEQUENCE": "SEQUENCE",
    "TABLESPACE": "TABLESPACE",
}
common_statements = {
    "INDEX": "INDEX",
    "REFERENCES": "REFERENCES",
    "KEY": "KEY",
    "ADD": "ADD",
    "AS": "AS",
    "CLONE": "CLONE",
    "DEFERRABLE": "DEFERRABLE",
    "INITIALLY": "INITIALLY",
    "IF": "IF",
    "NOT": "NOT",
    "EXISTS": "EXISTS",
    "ON": "ON",
    "FOR": "FOR",
    "ENCRYPT": "ENCRYPT",
    "SALT": "SALT",
    "NO": "NO",
    "USING": "USING",
    # bigquery
    "OPTIONS": "OPTIONS",
}

columns_definition = {
    "DELETE": "DELETE",
    "UPDATE": "UPDATE",
    "NULL": "NULL",
    "ARRAY": "ARRAY",
    ",": "COMMA",
    "DEFAULT": "DEFAULT",
    "COLLATE": "COLLATE",
    "ENFORCED": "ENFORCED",
    "ENCODE": "ENCODE",
    "GENERATED": "GENERATED",
    "COMMENT": "COMMENT",
    "TAG": "TAG",
    "POLICY": "POLICY",
    "MASKING": "MASKING",
    "MASKED": "MASKED",
    "WITH": "WITH",
    "ORDER": "ORDER",
    "NOORDER": "NOORDER"
}
first_liners = {
    "LIKE": "LIKE",
    "CONSTRAINT": "CONSTRAINT",
    "FOREIGN": "FOREIGN",
    "PRIMARY": "PRIMARY",
    "UNIQUE": "UNIQUE",
    "CHECK": "CHECK",
    "WITH": "WITH",
}

common_statements.update(first_liners)
definition_statements.update(common_statements)
after_columns_tokens = {
    "PARTITIONED": "PARTITIONED",
    "PARTITION": "PARTITION",
    "BY": "BY",
    # hql
    "INTO": "INTO",
    "STORED": "STORED",
    "LOCATION": "LOCATION",
    "ROW": "ROW",
    "FORMAT": "FORMAT",
    "TERMINATED": "TERMINATED",
    "COLLECTION": "COLLECTION",
    "ITEMS": "ITEMS",
    "MAP": "MAP",
    "KEYS": "KEYS",
    "SERDE": "SERDE",
    "CLUSTER": "CLUSTER",
    "SERDEPROPERTIES": "SERDEPROPERTIES",
    "TBLPROPERTIES": "TBLPROPERTIES",
    "USING": "USING",
    "SKEWED": "SKEWED",
    # oracle
    "STORAGE": "STORAGE",
    "TABLESPACE": "TABLESPACE",
    # mssql
    "TEXTIMAGE_ON": "TEXTIMAGE_ON",
    # psql
    "INHERITS": "INHERITS",
    # snowflake
    "DATA_RETENTION_TIME_IN_DAYS": "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS": "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    "CHANGE_TRACKING": "CHANGE_TRACKING",
}
sequence_reserved = {
    "INCREMENT": "INCREMENT",
    "START": "START",
    "WITH": "WITH",
    "MINVALUE": "MINVALUE",
    "MAXVALUE": "MAXVALUE",
    "CACHE": "CACHE",
    "NO": "NO",
    "BY": "BY",
    "NOORDER": "NOORDER",
    "ORDER": "ORDER"
}


tokens = tuple(
    set(
        [
            "ID",
            "DOT",
            "STRING_BASE",
            "DQ_STRING",
            "LP",
            "RP",
            "LT",
            "RT",
            "COMMAT",
            "AUTOINCREMENT",
        ]
        + list(definition_statements.values())
        + list(common_statements.values())
        + list(columns_definition.values())
        + list(sequence_reserved.values())
        + list(after_columns_tokens.values())
    )
)

symbol_tokens = {
    ")": "RP",
    "(": "LP",
}

symbol_tokens_no_check = {"<": "LT", ">": "RT"}
