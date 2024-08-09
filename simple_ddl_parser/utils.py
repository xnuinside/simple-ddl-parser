import re
from typing import Any, List, Optional, Tuple, Union

# Backward compatibility import
from simple_ddl_parser.exception import SimpleDDLParserException

__all__ = [
    "remove_par",
    "check_spec",
    "find_first_unpair_closed_par",
    "normalize_name",
    "get_table_id",
    "SimpleDDLParserException",
]

_parentheses = ("(", ")")


def remove_par(p_list: List[Union[str, Any]]) -> List[Union[str, Any]]:
    """
    Remove the parentheses from the given list

    Warn: p_list may contain unhashable types, such as 'dict'.
    """
    j = 0
    for i in range(len(p_list)):
        if p_list[i] not in _parentheses:
            p_list[j] = p_list[i]
            j += 1
    while j < len(p_list):
        p_list.pop()
    return p_list


_spec_mapper = {
    "'pars_m_t'": "'\t'",
    "'pars_m_n'": "'\n'",
    "'pars_m_dq'": '"',
    "pars_m_single": "'",
}


def check_spec(string: str) -> str:
    """
    Replace escape tokens to their representation
    """
    if string in _spec_mapper:
        return _spec_mapper[string]
    for replace_from, replace_to in _spec_mapper.items():
        if replace_from in string:
            return string.replace(replace_from, replace_to)
    return string


def find_first_unpair_closed_par(str_: str) -> Optional[int]:
    """
    Returns index of first unpair close parentheses.
    Or returns None, if there is no one.
    """
    count_open = 0
    for i, char in enumerate(str_):
        if char == "(":
            count_open += 1
        if char == ")":
            count_open -= 1
        if count_open < 0:
            return i
    return None


def normalize_name(name: str) -> str:
    """
    Clean up [] and " characters from the given name
    """
    clean_up_re = r'[\[\]"]'
    return re.sub(clean_up_re, "", name).lower()


def get_table_id(schema_name: str, table_name: str) -> Tuple[str, str]:
    table_name = normalize_name(table_name)
    if schema_name:
        schema_name = normalize_name(schema_name)
    return (table_name, schema_name)
