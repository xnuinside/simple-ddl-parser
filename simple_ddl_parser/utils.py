import re
from typing import List, Tuple, Optional, Union, Any

__all__ = [
    "remove_par",
    "check_spec",
    "find_first_unpair_closed_par",
    "normalize_name",
    "get_table_id",
]

_parentheses = ('(', ')')


def remove_par(p_list: List[Union[str, Any]]) -> List[Union[str, Any]]:
    """
    Remove the parentheses from the given list

    Warn: p_list may contain unhashable types for some unexplored reasons
    """
    i = j = 0
    while i < len(p_list):
        if p_list[i] not in _parentheses:
            p_list[j] = p_list[i]
            j += 1
        i += 1
    while j < len(p_list):
        p_list.pop()
    return p_list


spec_mapper = {
    "'pars_m_t'": "'\t'",
    "'pars_m_n'": "'\n'",
    "'pars_m_dq'": '"',
    "pars_m_single": "'",
}


# TODO: Add tests
def check_spec(value: str) -> str:
    replace_value = spec_mapper.get(value)
    if replace_value:
        return replace_value
    for item in spec_mapper:
        if item in value:
            return value.replace(item, spec_mapper[item])
    return value


def find_first_unpair_closed_par(str_: str) -> Optional[int]:
    count_open = 0
    for i, char in enumerate(str_):
        if char == '(':
            count_open += 1
        if char == ')':
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
