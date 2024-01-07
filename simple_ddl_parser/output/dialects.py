from typing import Dict, List

hql_clean_up_list = ["deferrable_initially"]


sql_clean_up_list = [
    "external",
    "external",
    "stored_as",
    "row_format",
    "lines_terminated_by",
    "fields_terminated_by",
    "collection_items_terminated_by",
    "map_keys_terminated_by",
]


def add_additional_hql_keys(table_data) -> Dict:
    table_data.if_not_exist_update(
        {
            "stored_as": None,
            "row_format": None,
            "fields_terminated_by": None,
            "lines_terminated_by": None,
            "fields_terminated_by": None,
            "map_keys_terminated_by": None,
            "collection_items_terminated_by": None,
            "external": table_data.get("external", False),
        }
    )
    return table_data


def add_additional_oracle_keys(table_data: Dict) -> Dict:
    table_data.if_not_exist_update(
        {
            "constraints": {"uniques": None, "checks": None, "references": None},
            "storage": None,
        }
    )
    return table_data


def update_bigquery_output(table_data: Dict) -> Dict:
    if table_data.get("schema") or table_data.get("sequences"):
        table_data["dataset"] = table_data["schema"]
        del table_data["schema"]
    return table_data


def add_additional_snowflake_keys(table_data: Dict) -> Dict:
    table_data.if_not_exist_update({"clone": None, "primary_key_enforced": None})
    return table_data


def add_additional_oracle_keys_in_column(column_data: Dict) -> Dict:
    column_data.if_not_exist_update({"encrypt": None})
    return column_data


def add_additional_snowflake_keys_in_column(column_data: Dict) -> Dict:
    return column_data


def add_additional_mssql_keys(table_data: Dict) -> Dict:
    table_data.if_not_exist_update(
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


def key_cleaning(table_data: Dict, output_mode: str) -> Dict:
    if output_mode != "hql":
        table_data = clean_up_output(table_data, sql_clean_up_list)
    else:
        table_data = clean_up_output(table_data, hql_clean_up_list)
        # todo: need to figure out how workaround it normally
        if "_ddl_parser_comma_only_str" == table_data.get("fields_terminated_by"):
            table_data["fields_terminated_by"] = "','"
    return table_data


def dialects_clean_up(output_mode: str, table_data) -> Dict:
    key_cleaning(table_data, output_mode)
    update_mappers_for_table_properties = {"bigquery": update_bigquery_output}
    update_table_prop = update_mappers_for_table_properties.get(output_mode)
    if update_table_prop:
        table_data = update_table_prop(table_data)

    return table_data
