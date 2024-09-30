# statements that used at the start of definition or in statements without columns
definition_statements = {
    "DROP",
    "CREATE",
    "TABLE",
    "DATABASE",
    "SCHEMA",
    "ALTER",
    "TYPE",
    "DOMAIN",
    "REPLACE",
    "OR",
    "CLUSTERED",
    "SEQUENCE",
    "TABLESPACE",
}

definition_statements = {value: value for value in definition_statements}


common_statements = {
    "INDEX",
    "REFERENCES",
    "KEY",
    "ADD",
    "AS",
    "CLONE",
    "DEFERRABLE",
    "INITIALLY",
    "IF",
    "NOT",
    "IN",
    "EXISTS",
    "ON",
    "FOR",
    "ENCRYPT",
    "SALT",
    "NO",
    "USING",
    "DELETE",
    "UPDATE",
    "DEFAULT",
    "COMMENT",
    # bigquery
    "OPTIONS",
    # snoflake
    "TAG",
    "IN",
}
common_statements = {value: value for value in common_statements}


columns_definition = {
    "ENUM",
    "SET",
    "NULL",
    "ARRAY",
    "COLLATE",
    "ENFORCED",
    "ENCODE",
    "GENERATED",
    "POLICY",
    "MASKING",
    "WITH",
    "WITHOUT",
    "ORDER",
    "NOORDER",
    "VISIBLE",
    "INVISIBLE",
    "AUTOINCREMENT",
    "GENERATED",
    "BY",
}
columns_definition = {value: value for value in columns_definition}
columns_definition[","] = "COMMA"


first_liners = {
    "LIKE",
    "CONSTRAINT",
    "FOREIGN",
    "PRIMARY",
    "INDEX",
    "UNIQUE",
    "CHECK",
    "WITH",
    "CLUSTER",
    "BY",
}
first_liners = {value: value for value in first_liners}


common_statements.update(first_liners)
definition_statements.update(common_statements)

alter_tokens = {"COLUMN", "RENAME", "PRIMARY", "KEY", "MODIFY"}
alter_tokens = {value: value for value in alter_tokens}

after_columns_tokens = {
    "PARTITIONED",
    "PARTITION",
    "BY",
    # hql
    "INTO",
    "STORED",
    "LOCATION",
    "ROW",
    "FORMAT",
    "TERMINATED",
    "COLLECTION",
    "ITEMS",
    "MAP",
    "KEYS",
    "SERDE",
    "CLUSTER",
    "SERDEPROPERTIES",
    "TBLPROPERTIES",
    "USING",
    "SKEWED",
    # oracle
    "STORAGE",
    "TABLESPACE",
    # mssql
    "TEXTIMAGE_ON",
    # psql
    "INHERITS",
    # snowflake
    "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    "CHANGE_TRACKING",
    "PATTERN",
    "AUTO_REFRESH",
    "FILE_FORMAT",
    "TABLE_FORMAT",
    "STAGE_FILE_FORMAT",
    "CATALOG",
    "ENGINE",
    "IN",
    "ESCAPED",
}
after_columns_tokens = {value: value for value in after_columns_tokens}


sequence_reserved = {
    "INCREMENT",
    "START",
    "WITH",
    "MINVALUE",
    "MAXVALUE",
    "CACHE",
    "NO",
    "BY",
    "NOORDER",
    "ORDER",
}
sequence_reserved = {value: value for value in sequence_reserved}


tokens = tuple(
    {
        *[
            "ID",
            "DOT",
            "STRING_BASE",
            "DQ_STRING",
            "LP",
            "RP",
            "LT",
            "RT",
            "COMMAT",
            "EQ",
        ],
        *definition_statements.values(),
        *common_statements.values(),
        *columns_definition.values(),
        *sequence_reserved.values(),
        *after_columns_tokens.values(),
        *alter_tokens.values(),
    }
)

symbol_tokens = {
    ")": "RP",
    "(": "LP",
}

symbol_tokens_no_check = {"<": "LT", ">": "RT"}
