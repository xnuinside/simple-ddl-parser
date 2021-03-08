import os
import json
from typing import Dict, List


def result_format(result: List[Dict]) -> List[Dict]:
    final_result = []
    for table in result:
        table_data = {"columns": [], "primary_key": None}
        for item in table:
            if item.get("table_name"):
                table_data["table_name"] = item["table_name"]
                table_data["schema"] = item["schema"]
            elif not item.get("type") and item.get("primary_key"):
                table_data["primary_key"] = item["primary_key"]
            else:
                table_data["columns"].append(item)
        if not table_data["primary_key"]:
            table_data = check_pk_in_columns(table_data)
        else:
            table_data = remove_pk_from_columns(table_data)
        final_result.append(table_data)
    return final_result


def remove_pk_from_columns(table_data: Dict):
    for column in table_data["columns"]:
        del column["primary_key"]
    return table_data


def check_pk_in_columns(table_data: Dict):
    pk = []
    for column in table_data["columns"]:
        if column["primary_key"]:
            pk.append(column["name"])
        del column["primary_key"]
    table_data["primary_key"] = pk
    return table_data


def dump_data_to_file(table_name: str, dump_path: str, data: List[Dict]) -> None:
    """ method to dump json schema """
    if not os.path.isdir(dump_path):
        os.makedirs(dump_path, exist_ok=True)
    with open(
        "{}/{}_schema.json".format(dump_path, table_name), "w+"
    ) as schema_file:
        json.dump(data, schema_file, indent=1)
