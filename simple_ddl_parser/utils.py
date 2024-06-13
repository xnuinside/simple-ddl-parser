import re
from typing import List, Tuple, Optional


def remove_par(p_list: List[str]) -> List[str]:
    remove_set = {"(", ")"}
    i = j = 0
    while i < len(p_list):
        if p_list[i] not in remove_set:
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


def check_spec(value: str) -> str:
    replace_value = spec_mapper.get(value)
    if not replace_value:
        for item in spec_mapper:
            if item in value:
                replace_value = value.replace(item, spec_mapper[item])
                break
        else:
            replace_value = value
    return replace_value


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
    # clean up [] and " symbols from names
    clean_up_re = r'[\[\]"]'
    return re.sub(clean_up_re, "", name).lower()


def get_table_id(schema_name: str, table_name: str) -> Tuple[str, str]:
    table_name = normalize_name(table_name)
    if schema_name:
        schema_name = normalize_name(schema_name)
    return (table_name, schema_name)
