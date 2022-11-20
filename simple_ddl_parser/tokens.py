# statements that used at the start of defenition or in statements without columns
defenition_statements = {
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

columns_defenition = {
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
    "COMMENT": "COMMENT"
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
defenition_statements.update(common_statements)
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
}
sequence_reserved = {
    "INCREMENT": "INCREMENT",
    "START": "START",
    "MINVALUE": "MINVALUE",
    "MAXVALUE": "MAXVALUE",
    "CACHE": "CACHE",
    "NO": "NO",
}


tokens = tuple(
    set(
        ["ID", "DOT", "STRING", "DQ_STRING", "LP", "RP", "LT", "RT", "COMMAT", "AUTOINCREMENT"]
        + list(defenition_statements.values())
        + list(common_statements.values())
        + list(columns_defenition.values())
        + list(sequence_reserved.values())
        + list(after_columns_tokens.values())
    )
)

symbol_tokens = {
    ")": "RP",
    "(": "LP",
}

symbol_tokens_no_check = {"<": "LT", ">": "RT"}
