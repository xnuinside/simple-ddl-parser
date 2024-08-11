from typing import List


class MSSQL:
    def p_alter_column_sql_server(self, p: List) -> None:
        """alter_column_sql_server : alt_table ALTER COLUMN defcolumn"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["columns_to_modify"] = [p_list[-1]]

    def p_pkey_constraint(self, p: List) -> None:
        """pkey_constraint : constraint pkey_statement id LP index_pid RP
        | constraint pkey_statement LP index_pid RP
        | pkey_constraint with
        | pkey_constraint with ON id
        """
        p_list = list(p)
        p[0] = p[1]
        if isinstance(p[2], dict) and "with" in p[2]:
            data = p_list[2]
            if "ON" in p_list:
                data["with"]["on"] = p_list[-1]
        elif len(p_list) == 7:
            data = {"primary_key": True, "columns": p_list[-2], p[3]: True}
        else:
            data = {"primary_key": True, "columns": p_list[-2]}

        p[0]["constraint"].update(data)

    def p_with(self, p: List) -> None:
        """with : WITH with_args"""
        p_list = list(p)
        p[0] = {"with": {"properties": [], "on": None}}
        if ")" not in p_list:
            p[0]["with"]["properties"] = p_list[-1]["properties"]

    def p_equals(self, p: List) -> None:
        """equals : id EQ id
        | id EQ ON
        | id EQ dot_id
        """
        p_list = list(p)
        if "." in p_list:
            p[0] = {"name": p_list[1], "value": f"{p_list[3]}.{p_list[5]}"}
        else:
            p[0] = {"name": p_list[-3], "value": p_list[-1]}

    def p_with_args(self, p: List) -> None:
        """with_args : LP equals
        | with_args COMMA equals
        | with_args with_args
        | with_args RP
        """
        p_list = list(p)
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {"properties": []}
        if ")" != p_list[2]:
            if ")" == p_list[-1]:
                p[0]["properties"].append(p_list[-1])
            else:
                p[0]["properties"].append(p_list[-1])

    def p_period_for(self, p: List) -> None:
        """period_for : id FOR id LP pid RP"""
        p[0] = {"period_for_system_time": p[5]}

    def p_expression_on_primary(self, p: List) -> None:
        """expr : expr ON id"""
        p[0] = p[1]
        p[0]["on"] = p[3]

    def p_expression_with(self, p: List) -> None:
        """expr : expr with"""
        p[0] = p[1]
        p[0].update(p[2])

    def p_expression_text_image_on(self, p: List) -> None:
        """expr : expr TEXTIMAGE_ON id"""
        p[0] = p[1]
        p[0].update({"textimage_on": p[3]})
