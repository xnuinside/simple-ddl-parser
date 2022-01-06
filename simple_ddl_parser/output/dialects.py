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
            "external": table_data.get("external", False),
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


def update_bigquery_output(table_data: Dict) -> Dict:
    if table_data.get("schema"):
        table_data["dataset"] = table_data["schema"]
        del table_data["schema"]
    return table_data


def add_additional_redshift_keys(table_data: Dict) -> Dict:
    table_data.update(
        {
            "diststyle": None,
            "distkey": None,
            "sortkey": {"type": None, "keys": []},
            "encode": None,
            "temp": False,
        }
    )
    return table_data


def add_additional_snowflake_keys(table_data: Dict) -> Dict:
    table_data.update({"clone": None, "primary_key_enforced": None})
    return table_data


def add_additional_oracle_keys_in_column(column_data: Dict) -> Dict:
    column_data.update({"encrypt": None})
    return column_data


def add_additional_snowflake_keys_in_column(column_data: Dict) -> Dict:
    return column_data


def add_additional_redshift_keys_in_column(column_data: Dict, table_data: Dict) -> Dict:
    column_data["encode"] = column_data.get("encode", None)
    if column_data.get("distkey"):
        table_data["distkey"] = column_data["name"]
        del column_data["distkey"]
    return column_data, table_data


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

    mehtod_mapper = {
        "hql": add_additional_hql_keys,
        "mssql": add_additional_mssql_keys,
        "mysql": add_additional_mssql_keys,
        "oracle": add_additional_oracle_keys,
        "redshift": add_additional_redshift_keys,
        "snowflake": add_additional_snowflake_keys,
    }

    method = mehtod_mapper.get(output_mode)

    if method:
        table_data = method(table_data)

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


def process_redshift_dialect(table_data: List[Dict]) -> List[Dict]:
    for column in table_data.get("columns", []):
        column, table_data = add_additional_redshift_keys_in_column(column, table_data)
        if table_data.get("encode"):
            column["encode"] = column["encode"] or table_data.get("encode")
    return table_data


def dialects_clean_up(output_mode: str, table_data: Dict) -> Dict:
    key_cleaning(table_data, output_mode)
    update_mappers_for_table_properties = {"bigquery": update_bigquery_output}
    update_table_prop = update_mappers_for_table_properties.get(output_mode)

    if update_table_prop:
        table_data = update_table_prop(table_data)

    if output_mode == "oracle":
        for column in table_data["columns"]:
            column = add_additional_oracle_keys_in_column(column)
    elif output_mode == "snowflake":
        for column in table_data["columns"]:
            column = add_additional_snowflake_keys_in_column(column)
    elif output_mode == "redshift":
        table_data = process_redshift_dialect(table_data)
    return table_data
