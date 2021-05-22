# statements that used at the start of defenition or in statements without columns
defenition_statements = {
    "DROP": "DROP",
    "CREATE": "CREATE",
    "TABLE": "TABLE",
    "ALTER": "ALTER",
    "TYPE": "TYPE",
    "DOMAIN": "DOMAIN",
    "REPLACE": "REPLACE",
    "OR": "OR",
    "CLUSTERED": "CLUSTERED",
    "SEQUENCE": "SEQUENCE",
}
common_statements = {
    "CHECK": "CHECK",
    "CONSTRAINT": "CONSTRAINT",
    "FOREIGN": "FOREIGN",
    "INDEX": "INDEX",
    "REFERENCES": "REFERENCES",
    "KEY": "KEY",
    "ADD": "ADD",
    "AS": "AS",
    "LIKE": "LIKE",
    "DEFERRABLE": "DEFERRABLE",
    "INITIALLY": "INITIALLY",
    "IF": "IF",
    "NOT": "NOT",
    "EXISTS": "EXISTS",
    "UNIQUE": "UNIQUE",
    "ON": "ON",
    "FOR": "FOR",
    "ENCRYPT": "ENCRYPT",
    "SALT": "SALT",
    "NO": "NO",
    "USING": "USING",
}

columns_defenition = {
    "DELETE": "DELETE",
    "UPDATE": "UPDATE",
    "NULL": "NULL",
    "PRIMARY": "PRIMARY",
    "ARRAY": "ARRAY",
    ",": "COMMA",
    "DEFAULT": "DEFAULT",
    "GENERATED": "GENERATED",
}

after_columns_tokens = {
    "PARTITIONED": "PARTITIONED",
    "BY": "BY",
    # hql
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
    "WITH": "WITH",
    "SERDEPROPERTIES": "SERDEPROPERTIES",
    # oracle
    "STORAGE": "STORAGE",
    "TABLESPACE": "TABLESPACE",
}
sequence_reserved = {
    "INCREMENT": "INCREMENT",
    "START": "START",
    "MINVALUE": "MINVALUE",
    "MAXVALUE": "MAXVALUE",
    "CACHE": "CACHE",
}


tokens = tuple(
    ["ID", "DOT", "STRING", "LP", "RP", "LT", "RT", "COMMAT"]
    + list(defenition_statements.values())
    + list(common_statements.values())
    + list(columns_defenition.values())
    + list(sequence_reserved.values())
    + list(after_columns_tokens.values())
)

symbol_tokens = {
    ")": "RP",
    "(": "LP",
}

symbol_tokens_no_check = {"<": "LT", ">": "RT"}
