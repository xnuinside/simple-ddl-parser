"""Parsing of statements that wrap a query: CREATE VIEW and Snowflake
CREATE DYNAMIC TABLE.

The query body (``AS <query>``) can contain any SQL, so these statements are
handled outside of the yacc grammar: the header is parsed with a small token
walker and the query is kept as text plus a best-effort lineage extraction
(output columns and source tables).
"""

import re
from typing import NamedTuple

_IDENT_PART = r'(?:"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|[\w$@#]+)'

TOKEN_RE = re.compile(
    rf"""
    (?P<str>'(?:[^'\\]|\\.|'')*')
    |(?P<word>{_IDENT_PART}(?:\s*\.\s*(?:{_IDENT_PART}|\*))*)
    |(?P<lp>\()
    |(?P<rp>\))
    |(?P<comma>,)
    |(?P<op>::|<=|>=|<>|!=|\|\||=>|->>|->|\S)
    """,
    flags=re.VERBOSE,
)


class Token(NamedTuple):
    kind: str
    value: str
    start: int
    end: int

    @property
    def upper(self) -> str:
        return self.value.upper() if self.kind == "word" else self.value


# words that can not be an implicit alias of a select item or a table
RESERVED_WORDS = {
    "ALL",
    "AND",
    "ANTI",
    "AS",
    "ASOF",
    "BETWEEN",
    "BY",
    "CASE",
    "CONNECT",
    "CROSS",
    "DISTINCT",
    "ELSE",
    "END",
    "EXCEPT",
    "FALSE",
    "FETCH",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "ILIKE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "MATCH_CONDITION",
    "MINUS",
    "NATURAL",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PIVOT",
    "QUALIFY",
    "RIGHT",
    "SAMPLE",
    "SELECT",
    "SEMI",
    "START",
    "TABLESAMPLE",
    "THEN",
    "TRUE",
    "UNION",
    "UNPIVOT",
    "USING",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
}

# keywords after which a space before "(" is kept when normalizing text
_KEEP_SPACE_BEFORE_LP = RESERVED_WORDS | {"EXISTS", "VALUES"}

_VIEW_MODIFIERS = {
    "SECURE",
    "MATERIALIZED",
    "LOCAL",
    "GLOBAL",
    "TEMP",
    "TEMPORARY",
    "VOLATILE",
    "RECURSIVE",
}
_DYNAMIC_TABLE_MODIFIERS = {"TRANSIENT", "DYNAMIC"}

_COLUMN_OPTION_WORDS = {
    "COMMENT",
    "WITH",
    "MASKING",
    "PROJECTION",
    "TAG",
    "NOT",
    "NULL",
}


def tokenize(text: str) -> list[Token]:
    tokens = []
    for match in TOKEN_RE.finditer(text):
        kind = match.lastgroup
        tokens.append(Token(kind, match.group(kind), match.start(), match.end()))
    return tokens


def normalize_sql_text(tokens: list[Token]) -> str:
    """rebuild text from tokens with readable spacing: no spaces inside
    parentheses, "a, b" for commas, "f(x)" for function calls"""
    result = ""
    prev: Token | None = None
    for token in tokens:
        if prev is None or token.kind in ("rp", "comma") or prev.kind == "lp":
            sep = ""
        elif token.kind == "lp":
            sep = " " if prev.upper in _KEEP_SPACE_BEFORE_LP else ""
            if prev.kind in ("op", "comma"):
                sep = " "
        elif token.value == "::" or prev.value == "::":
            sep = ""
        else:
            sep = " "
        result += sep + token.value
        prev = token
    return result


def _matching_rp(tokens: list[Token], lp_index: int) -> int:
    depth = 0
    for index in range(lp_index, len(tokens)):
        if tokens[index].kind == "lp":
            depth += 1
        elif tokens[index].kind == "rp":
            depth -= 1
            if depth == 0:
                return index
    return len(tokens) - 1


def _split_by_comma(tokens: list[Token]) -> list[list[Token]]:
    items: list[list[Token]] = [[]]
    depth = 0
    for token in tokens:
        if token.kind == "lp":
            depth += 1
        elif token.kind == "rp":
            depth -= 1
        if token.kind == "comma" and depth == 0:
            items.append([])
        else:
            items[-1].append(token)
    return [item for item in items if item]


def split_identifier(identifier: str) -> list[str]:
    return [
        part.strip()
        for part in re.findall(r'"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|[^.\s]+', identifier)
    ]


def _identifier_dict(identifier: str, name_key: str = "table_name") -> dict:
    parts = split_identifier(identifier)
    data = {name_key: parts[-1], "schema": None}
    if len(parts) > 1:
        data["schema"] = parts[-2]
    if len(parts) > 2:
        data["database"] = parts[-3]
    return data


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _find_query_start(tokens: list[Token]) -> int | None:
    """index of the first token of the query: the token after the first
    top level AS that is followed by SELECT, WITH or ("""
    depth = 0
    for index, token in enumerate(tokens[:-1]):
        if token.kind == "lp":
            depth += 1
        elif token.kind == "rp":
            depth -= 1
        elif depth == 0 and token.upper == "AS":
            next_token = tokens[index + 1]
            if next_token.kind == "lp" or next_token.upper in ("SELECT", "WITH"):
                return index + 1
    return None


def _format_tags(tokens: list[Token]):
    """WITH TAG (a.b = 'x', c = 'y') -> "a.b='x'" or ["a.b='x'", "c='y'"]"""
    tags = ["".join(token.value for token in item) for item in _split_by_comma(tokens)]
    return tags[0] if len(tags) == 1 else tags


def _parse_size(tokens: list[Token]):
    values = []
    for item in _split_by_comma(tokens):
        value = "".join(token.value for token in item)
        values.append(int(value) if value.isdigit() else value)
    if len(values) == 1:
        return values[0]
    return tuple(values)


class _HeaderParser:
    """walks tokens of the statement header (everything before AS <query>)"""

    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    def peek(self, offset: int = 0) -> Token | None:
        index = self.pos + offset
        if index < len(self.tokens):
            return self.tokens[index]
        return None

    def peek_upper(self, offset: int = 0) -> str:
        token = self.peek(offset)
        return token.upper if token else ""

    def accept(self, *words: str) -> bool:
        """consume words if all of them are next in tokens"""
        for offset, word in enumerate(words):
            if self.peek_upper(offset) != word:
                return False
        self.pos += len(words)
        return True

    def next(self) -> Token | None:
        token = self.peek()
        if token is not None:
            self.pos += 1
        return token

    def done(self) -> bool:
        return self.pos >= len(self.tokens)

    def take_parens(self) -> list[Token] | None:
        """consume "( ... )" and return inner tokens"""
        token = self.peek()
        if token is None or token.kind != "lp":
            return None
        end = _matching_rp(self.tokens, self.pos)
        inner = self.tokens[self.pos + 1 : end]
        self.pos = end + 1
        return inner

    def take_value(self) -> str | None:
        """value after optional '=': string, word or (...) group"""
        self.accept("=")
        token = self.peek()
        if token is None:
            return None
        if token.kind == "lp":
            return normalize_sql_text(self.take_parens() or [])
        self.pos += 1
        if token.kind == "str":
            return _unquote(token.value)
        return token.value


def _parse_column_options(parser: _HeaderParser, column: dict) -> bool:
    """parse one column option; return False if no option was recognized"""
    if parser.accept("COMMENT"):
        parser.accept("=")
        token = parser.next()
        if token is None:
            return False
        column["comment"] = token.value
    elif parser.accept("WITH", "MASKING", "POLICY") or parser.accept(
        "MASKING", "POLICY"
    ):
        policy = parser.next()
        if policy is None:
            return False
        column["with_masking_policy"] = policy.value
        if parser.accept("USING"):
            column["masking_policy_using"] = [
                normalize_sql_text(item)
                for item in _split_by_comma(parser.take_parens() or [])
            ]
    elif parser.accept("WITH", "PROJECTION", "POLICY") or parser.accept(
        "PROJECTION", "POLICY"
    ):
        policy = parser.next()
        if policy is None:
            return False
        column["with_projection_policy"] = policy.value
    elif parser.accept("WITH", "TAG") or parser.accept("TAG"):
        column["with_tag"] = _format_tags(parser.take_parens() or [])
    elif parser.accept("NOT", "NULL"):
        column["nullable"] = False
    elif parser.accept("NULL"):
        column["nullable"] = True
    else:
        return False
    return True


# keys of a regular table column, used for dynamic table columns
_TYPED_COLUMN_DEFAULTS = {
    "type": None,
    "size": None,
    "primary_key": False,
    "references": None,
    "unique": False,
    "nullable": True,
    "default": None,
    "check": None,
}


def _merge_columns(
    declared: list[dict], query_columns: list[dict], with_types: bool
) -> list[dict]:
    """one entry per output column: declared column attributes + expression and
    source of the matching select item. Declared columns are matched by
    position, only when the select list has no star and the same length."""
    if not declared:
        columns = []
        for query_column in query_columns:
            column = {"name": query_column["name"]}
            if with_types:
                if column["name"] is None:
                    column["name"] = query_column["expression"]
                column.update(_TYPED_COLUMN_DEFAULTS)
            column["expression"] = query_column["expression"]
            column["source"] = query_column["source"]
            columns.append(column)
        return columns

    positional = len(declared) == len(query_columns) and not any(
        query_column["expression"].endswith("*") for query_column in query_columns
    )
    for index, column in enumerate(declared):
        query_column = query_columns[index] if positional else None
        column["expression"] = query_column["expression"] if query_column else None
        column["source"] = query_column["source"] if query_column else None
    return declared


def _parse_columns(tokens: list[Token], with_types: bool) -> list[dict] | None:
    columns = []
    for item in _split_by_comma(tokens):
        parser = _HeaderParser(item)
        name = parser.next()
        column: dict = {"name": name.value}
        if with_types:
            column.update(_TYPED_COLUMN_DEFAULTS)
            token = parser.peek()
            if (
                token is not None
                and token.kind == "word"
                and token.upper not in _COLUMN_OPTION_WORDS
            ):
                parser.next()
                column["type"] = token.value
                if parser.peek() is not None and parser.peek().kind == "lp":
                    column["size"] = _parse_size(parser.take_parens())
        while not parser.done():
            if not _parse_column_options(parser, column):
                return None
        columns.append(column)
    return columns


_INT_PROPERTIES = {
    "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS",
}


def _parse_columns_and_properties(
    parser: _HeaderParser, data: dict, with_types: bool
) -> list[dict] | None:
    """parse the optional column list and object properties after the name;
    the column list can follow properties (MATERIALIZED VIEW ... COPY GRANTS (...))
    return declared columns or None if the header is not supported"""
    columns: list[dict] = []
    columns_parsed = False
    while not parser.done():
        token = parser.peek()
        if token.kind == "lp" and not columns_parsed:
            parsed = _parse_columns(parser.take_parens(), with_types)
            if parsed is None:
                return None
            columns = parsed
            columns_parsed = True
        elif not _parse_property(parser, data):
            return None
    return columns


def _parse_property(parser: _HeaderParser, data: dict) -> bool:
    """parse one object property; return False if it was not recognized"""
    if parser.accept("COPY", "GRANTS"):
        data["copy_grants"] = True
    elif parser.accept("COMMENT"):
        parser.accept("=")
        token = parser.next()
        if token is None:
            return False
        data["comment"] = token.value
    elif parser.accept("WITH", "ROW", "ACCESS", "POLICY") or parser.accept(
        "ROW", "ACCESS", "POLICY"
    ):
        policy = parser.next()
        if policy is None:
            return False
        data["with_row_access_policy"] = policy.value
        if parser.accept("ON"):
            data["row_access_policy_on"] = [
                normalize_sql_text(item)
                for item in _split_by_comma(parser.take_parens() or [])
            ]
    elif parser.accept("WITH", "TAG") or parser.accept("TAG"):
        data["with_tag"] = _format_tags(parser.take_parens() or [])
    elif parser.accept("WITH", "AGGREGATION", "POLICY") or parser.accept(
        "AGGREGATION", "POLICY"
    ):
        policy = parser.next()
        if policy is None:
            return False
        data["with_aggregation_policy"] = policy.value
    elif parser.accept("CLUSTER", "BY"):
        data["cluster_by"] = [
            normalize_sql_text(item)
            for item in _split_by_comma(parser.take_parens() or [])
        ]
    elif parser.accept("REQUIRE", "USER"):
        data["require_user"] = True
    elif parser.accept("IMMUTABLE", "WHERE"):
        data["immutable_where"] = normalize_sql_text(parser.take_parens() or [])
    elif parser.accept("BACKFILL", "FROM"):
        source = parser.next()
        if source is None:
            return False
        data["backfill_from"] = source.value
    elif parser.peek_upper(1) == "=" and parser.peek().kind == "word":
        # generic KEY = value property: TARGET_LAG, WAREHOUSE, REFRESH_MODE...
        key = parser.next().upper
        value = parser.take_value()
        if key in _INT_PROPERTIES and value is not None and value.isdigit():
            data[key.lower()] = int(value)
        elif key == "CHANGE_TRACKING" and value is not None:
            data["change_tracking"] = value.upper() == "TRUE"
        else:
            data[key.lower()] = value
    else:
        return False
    return True


def _parse_create_header(
    tokens: list[Token], object_words: tuple[str, ...], modifiers: set
) -> tuple[dict, list[str], _HeaderParser] | None:
    parser = _HeaderParser(tokens)
    if not parser.accept("CREATE"):
        return None
    data: dict = {}
    data["replace"] = parser.accept("OR", "REPLACE")
    found_modifiers = []
    while parser.peek_upper() in modifiers:
        found_modifiers.append(parser.next().upper)
    if not parser.accept(*object_words):
        return None
    if parser.accept("IF", "NOT", "EXISTS"):
        data["if_not_exists"] = True
    name = parser.next()
    if name is None or name.kind != "word":
        return None
    data["__name__"] = name.value
    return data, found_modifiers, parser


def parse_create_view(statement: str) -> dict | None:
    """CREATE [OR REPLACE] [SECURE] [TEMPORARY] [RECURSIVE] [MATERIALIZED]
    VIEW [IF NOT EXISTS] name [(columns)] [properties] AS query"""
    tokens = tokenize(statement)
    query_start = _find_query_start(tokens)
    if query_start is None:
        return None
    header = _parse_create_header(tokens[: query_start - 1], ("VIEW",), _VIEW_MODIFIERS)
    if header is None:
        return None
    data, modifiers, parser = header

    identifier = _identifier_dict(data.pop("__name__"), name_key="view_name")
    view = {
        "schema": identifier["schema"],
        "view_name": identifier["view_name"],
    }
    if "database" in identifier:
        view["database"] = identifier["database"]
    view["replace"] = data["replace"]
    if data.get("if_not_exists"):
        view["if_not_exists"] = True
    if "SECURE" in modifiers:
        view["secure"] = True
    if "RECURSIVE" in modifiers:
        view["recursive"] = True
    if "MATERIALIZED" in modifiers:
        view["materialized"] = True
    temporary = [
        item
        for item in modifiers
        if item not in ("SECURE", "RECURSIVE", "MATERIALIZED")
    ]
    if temporary:
        view["temporary"] = " ".join(temporary)

    declared_columns = _parse_columns_and_properties(parser, view, with_types=False)
    if declared_columns is None:
        return None

    query_tokens = tokens[query_start:]
    view["definition"] = normalize_sql_text(query_tokens)
    query_columns, source_tables = extract_query_lineage(query_tokens)
    view["columns"] = _merge_columns(declared_columns, query_columns, False)
    view["source_tables"] = source_tables
    return view


def parse_create_dynamic_table(statement: str) -> dict | None:
    """CREATE [OR REPLACE] [TRANSIENT] DYNAMIC TABLE [IF NOT EXISTS] name
    [(columns)] TARGET_LAG = ... WAREHOUSE = ... [properties] AS query"""
    tokens = tokenize(statement)
    query_start = _find_query_start(tokens)
    if query_start is None:
        return None
    header = _parse_create_header(
        tokens[: query_start - 1], ("TABLE",), _DYNAMIC_TABLE_MODIFIERS
    )
    if header is None:
        return None
    data, modifiers, parser = header
    if "DYNAMIC" not in modifiers:
        return None

    identifier = _identifier_dict(data.pop("__name__"))
    table = {
        "table_name": identifier["table_name"],
        "schema": identifier["schema"],
        "primary_key": [],
        "columns": [],
        "alter": {},
        "checks": [],
        "index": [],
        "partitioned_by": [],
        "tablespace": None,
        "dynamic": True,
    }
    if "database" in identifier:
        # same key as used for 3-part names of regular tables
        table["project"] = identifier["database"]
    if data["replace"]:
        table["replace"] = True
    if data.get("if_not_exists"):
        table["if_not_exists"] = True
    if "TRANSIENT" in modifiers:
        table["transient"] = True

    declared_columns = _parse_columns_and_properties(parser, table, with_types=True)
    if declared_columns is None:
        return None

    query_tokens = tokens[query_start:]
    table["query"] = normalize_sql_text(query_tokens)
    query_columns, source_tables = extract_query_lineage(query_tokens)
    table["columns"] = _merge_columns(declared_columns, query_columns, True)
    table["source_tables"] = source_tables
    return table


def _skip_ctes(tokens: list[Token]) -> tuple[int, list[str]]:
    """return index of the main query after WITH ... clauses and CTE names"""
    if not tokens or tokens[0].upper != "WITH":
        return 0, []
    pos = 1
    if pos < len(tokens) and tokens[pos].upper == "RECURSIVE":
        pos += 1
    names = []
    while pos < len(tokens):
        names.append(tokens[pos].value)
        pos += 1
        if pos < len(tokens) and tokens[pos].kind == "lp":
            pos = _matching_rp(tokens, pos) + 1
        if pos < len(tokens) and tokens[pos].upper == "AS":
            pos += 1
        if pos < len(tokens) and tokens[pos].kind == "lp":
            pos = _matching_rp(tokens, pos) + 1
        if pos < len(tokens) and tokens[pos].kind == "comma":
            pos += 1
            continue
        break
    return pos, names


def _query_depths(tokens: list[Token]) -> list[int | None]:
    """for each token: paren depth if the token is in a query context
    (top level or a subquery), None if inside a function call / expression"""
    stack = [True]
    result: list[int | None] = []
    for index, token in enumerate(tokens):
        if token.kind == "lp":
            result.append(len(stack) - 1 if stack[-1] else None)
            next_upper = tokens[index + 1].upper if index + 1 < len(tokens) else ""
            stack.append(next_upper in ("SELECT", "WITH"))
            continue
        if token.kind == "rp":
            if len(stack) > 1:
                stack.pop()
            result.append(len(stack) - 1 if stack[-1] else None)
            continue
        result.append(len(stack) - 1 if stack[-1] else None)
    return result


_TABLE_REF_END = RESERVED_WORDS | {"LIMIT", "SAMPLE", "AT", "BEFORE", "CHANGES"}


def _extract_table_refs(
    tokens: list[Token], depths: list[int | None]
) -> list[tuple[str, str | None, int | None]]:
    """(identifier, alias, depth) for each table after FROM / JOIN"""
    refs = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if depths[index] is None or token.upper not in ("FROM", "JOIN"):
            index += 1
            continue
        depth = depths[index]
        index += 1
        while index < len(tokens):
            ref = tokens[index]
            following = tokens[index + 1] if index + 1 < len(tokens) else None
            if (
                ref.kind != "word"
                or ref.upper in RESERVED_WORDS
                or (following is not None and following.kind == "lp")
            ):
                # subquery, LATERAL, TABLE(...) function, etc.
                break
            alias = None
            index += 1
            if index < len(tokens) and tokens[index].upper == "AS":
                index += 1
            if (
                index < len(tokens)
                and tokens[index].kind == "word"
                and tokens[index].upper not in _TABLE_REF_END
            ):
                alias = tokens[index].value
                index += 1
            refs.append((ref.value, alias, depth))
            if (
                token.upper == "FROM"
                and index < len(tokens)
                and tokens[index].kind == "comma"
                and depths[index] == depth
            ):
                index += 1
                continue
            break
    return refs


def _select_items(
    tokens: list[Token], start: int, depths: list[int | None]
) -> list[list[Token]]:
    """tokens of each item of the select list of SELECT at index start"""
    depth = depths[start]
    pos = start + 1
    while pos < len(tokens) and tokens[pos].upper in ("DISTINCT", "ALL"):
        pos += 1
    if pos < len(tokens) and tokens[pos].upper == "TOP":
        pos += 2
    end = pos
    stop_words = {"FROM", "UNION", "EXCEPT", "MINUS", "INTERSECT", "WHERE"}
    while end < len(tokens):
        if depths[end] == depth and tokens[end].upper in stop_words:
            break
        if (
            tokens[end].kind == "rp"
            and depths[end] is not None
            and depths[end] < depth
        ):
            break
        end += 1
    return _split_by_comma(tokens[pos:end])


def _select_item_to_column(
    item: list[Token],
    aliases: dict[str, str],
    main_tables: list[str],
) -> dict:
    alias = None
    expression_tokens = item
    if len(item) >= 3 and item[-2].upper == "AS":
        alias = item[-1].value
        expression_tokens = item[:-2]
    elif (
        len(item) >= 2
        and item[-1].kind == "word"
        and item[-1].upper not in RESERVED_WORDS
        and item[-2].kind in ("word", "str", "rp")
        and item[-2].upper not in RESERVED_WORDS - {"END"}
    ):
        alias = item[-1].value
        expression_tokens = item[:-1]

    expression = normalize_sql_text(expression_tokens)
    source = None
    name = alias
    if len(expression_tokens) == 1 and (
        expression_tokens[0].kind == "word" or expression_tokens[0].value == "*"
    ):
        parts = split_identifier(expression_tokens[0].value)
        column_name = parts[-1]
        if name is None:
            name = column_name
        if len(parts) > 1:
            qualifier = ".".join(parts[:-1])
            table = aliases.get(qualifier.lower(), qualifier)
        elif len(main_tables) == 1:
            table = main_tables[0]
        else:
            table = None
        if table is not None:
            source = _identifier_dict(table)
            source["column"] = column_name
    return {"name": name, "expression": expression, "source": source}


def extract_query_lineage(tokens: list[Token]) -> tuple[list[dict], list[dict]]:
    """best-effort extraction of the output columns of the main SELECT and of
    the physical tables read by the query"""
    if not tokens:
        return [], []
    depths = _query_depths(tokens)
    main_start, cte_names = _skip_ctes(tokens)
    cte_lower = {name.lower() for name in cte_names}

    refs = _extract_table_refs(tokens, depths)
    aliases: dict[str, str] = {}
    source_tables: list[dict] = []
    seen = set()
    for identifier, alias, _ in refs:
        last_part = split_identifier(identifier)[-1].lower()
        aliases.setdefault(last_part, identifier)
        aliases.setdefault(identifier.lower(), identifier)
        if alias:
            aliases[alias.lower()] = identifier
        if identifier.lower() in cte_lower or identifier.lower() in seen:
            continue
        seen.add(identifier.lower())
        source_tables.append(_identifier_dict(identifier))

    select_index = None
    for index in range(main_start, len(tokens)):
        if tokens[index].upper == "SELECT" and depths[index] is not None:
            select_index = index
            break
    query_columns = []
    if select_index is not None:
        select_depth = depths[select_index]
        main_tables = []
        for identifier, _, depth in refs:
            if depth == select_depth and identifier not in main_tables:
                main_tables.append(identifier)
        for item in _select_items(tokens, select_index, depths):
            query_columns.append(_select_item_to_column(item, aliases, main_tables))
    return query_columns, source_tables
