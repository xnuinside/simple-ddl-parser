from typing import Dict, List

hql_clean_up_list = ["deferrable_initially"]


sql_clean_up_list = [
    "external",
    "external",
    "stored_as",
    "location",
    "row_format",
    "lines_terminated_by",
    "fields_terminated_by",
    "collection_items_terminated_by",
    "map_keys_terminated_by",
    "comment",
]


def add_additional_hql_keys(table_data: Dict) -> Dict:
    table_data.update(
        {
            "stored_as": None,
            "location": None,
            "comment": None,
            "row_format": None,
            "fields_terminated_by": None,
            "lines_terminated_by": None,
            "fields_terminated_by": None,
            "map_keys_terminated_by": None,
            "collection_items_terminated_by": None,
        }
    )
    return table_data


def add_additional_oracle_keys(table_data: Dict) -> Dict:
    table_data.update(
        {
            "constraints": {"uniques": None, "checks": None, "references": None},
            "storage": None,
        }
    )
    return table_data


def add_additional_oracle_keys_in_column(table_data: Dict) -> Dict:
    table_data.update({"encrypt": None})
    return table_data


def add_additional_mssql_keys(table_data: Dict) -> Dict:
    table_data.update(
        {
            "constraints": {"uniques": None, "checks": None, "references": None},
        }
    )
    return table_data


def clean_up_output(table_data: Dict, key_list: List[str]) -> Dict:
    for key in key_list:
        if key in table_data:
            del table_data[key]
    return table_data


def populate_dialects_table_data(output_mode: str, table_data: Dict) -> Dict:
    if output_mode == "hql":
        table_data = add_additional_hql_keys(table_data)
    elif output_mode in ["mssql", "mysql"]:
        table_data = add_additional_mssql_keys(table_data)
    elif output_mode == "oracle":
        table_data = add_additional_oracle_keys(table_data)
    return table_data


def dialects_clean_up(output_mode: str, table_data: Dict) -> Dict:
    if output_mode != "hql":
        table_data = clean_up_output(table_data, sql_clean_up_list)
    else:
        table_data = clean_up_output(table_data, hql_clean_up_list)
        # todo: need to figure out how workaround it normally
        if "_ddl_parser_comma_only_str" == table_data["fields_terminated_by"]:
            table_data["fields_terminated_by"] = ","
    if output_mode == "oracle":
        for column in table_data["columns"]:
            column = add_additional_oracle_keys_in_column(column)
    return table_data
