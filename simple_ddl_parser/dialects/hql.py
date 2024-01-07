from typing import List

from simple_ddl_parser.utils import check_spec, remove_par


class HQL:
    def p_expression_location(self, p: List) -> None:
        """expr : expr LOCATION STRING
        | expr LOCATION DQ_STRING
        | expr LOCATION table_property_equals"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["location"] = p_list[-1]

    def p_expression_clustered(self, p: List) -> None:
        """expr : expr ID ON LP pid RP
        | expr ID by_smthg"""
        p[0] = p[1]
        p_list = list(p)
        if isinstance(p_list[-1], dict):
            # mean we 'in by' statement
            p[0][f"{p_list[-2].lower()}_by"] = list(p_list[-1].values())[0]
        else:
            p[0][f"{p_list[2].lower()}_{p_list[3].lower()}"] = p_list[-2]

    def p_expression_into_buckets(self, p: List) -> None:
        """expr : expr INTO ID ID"""
        p[0] = p[1]
        p_list = list(p)
        p[0][f"{p_list[2].lower()}_{p_list[-1].lower()}"] = p_list[-2]

    def p_row_format(self, p: List) -> None:
        """row_format : ROW FORMAT SERDE
        | ROW FORMAT
        """
        p_list = list(p)
        p[0] = {"serde": p_list[-1] == "SERDE"}

    def p_expression_row_format(self, p: List) -> None:
        """expr : expr row_format id
        | expr row_format STRING
        """
        p[0] = p[1]
        p_list = list(p)
        if p[2]["serde"]:
            format = {"serde": True, "java_class": p_list[-1]}
        else:
            format = check_spec(p_list[-1])

        p[0]["row_format"] = format

    def p_expression_with_serde(self, p: List) -> None:
        """expr : expr WITH SERDEPROPERTIES multi_assignments"""
        p[0] = p[1]
        p_list = list(p)

        row_format = p[0]["row_format"]
        row_format["properties"] = p_list[-1]
        p[0]["row_format"] = row_format

    def p_expression_tblproperties(self, p: List) -> None:
        """expr : expr TBLPROPERTIES multi_assignments"""
        p[0] = p[1]
        p[0]["tblproperties"] = list(p)[-1]

    def p_multi_assignments(self, p: List) -> None:
        """multi_assignments : LP assignment
        | multi_assignments RP
        | multi_assignments COMMA assignment"""
        p_list = remove_par(list(p))
        p[0] = p_list[1]
        p[0].update(p_list[-1])

    def p_assignment(self, p: List) -> None:
        """assignment : id id id
        |  STRING id STRING
        |  id id STRING
        |  STRING id id
        |  STRING id"""
        p_list = remove_par(list(p))
        if "state" in self.lexer.__dict__:
            p[0] = {p[1]: self.lexer.state.get(p_list[-1])}
        else:
            if "=" in p_list[-1]:
                p_list[-1] = p_list[-1].split("=")[-1]
            p[0] = {p_list[1]: p_list[-1]}

    def p_expression_comment(self, p: List) -> None:
        """expr : expr COMMENT STRING"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["comment"] = check_spec(p_list[-1])

    def p_expression_terminated_by(self, p: List) -> None:
        """expr : expr id TERMINATED BY id
        | expr id TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0][f"{p[2].lower()}_terminated_by"] = check_spec(p_list[-1])

    def p_expression_map_keys_terminated_by(self, p: List) -> None:
        """expr : expr MAP KEYS TERMINATED BY id
        | expr MAP KEYS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["map_keys_terminated_by"] = check_spec(p_list[-1])

    def p_expression_skewed_by(self, p: List) -> None:
        """expr : expr SKEWED BY LP id RP ON LP pid RP"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["skewed_by"] = {"key": p_list[4], "on": p_list[-1]}

    def p_expression_collection_terminated_by(self, p: List) -> None:
        """expr : expr COLLECTION ITEMS TERMINATED BY id
        | expr COLLECTION ITEMS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["collection_items_terminated_by"] = check_spec(p_list[-1])

    def p_expression_stored_as(self, p: List) -> None:
        """expr : expr STORED AS id
        |  expr STORED AS id STRING
        |  expr STORED AS id STRING id STRING
        """
        p[0] = p[1]
        p_list = list(p)
        if len(p_list) >= 6:
            # only input or output format
            p[0]["stored_as"] = {p_list[-2].lower(): p_list[-1]}
            if len(p_list) == 8:
                # both input & output
                p[0]["stored_as"].update({p_list[-4].lower(): p_list[-3]})
        else:
            p[0]["stored_as"] = p_list[-1]

    def p_expression_partitioned_by_hql(self, p: List) -> None:
        """expr : expr PARTITIONED BY pid_with_type
        | expr PARTITIONED BY LP pid RP
        | expr PARTITIONED BY LP multiple_funct RP
        """
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["partitioned_by"] = p_list[-1]

    def p_pid_with_type(self, p: List) -> None:
        """pid_with_type :  LP column
        | pid_with_type COMMA column
        | pid_with_type RP
        """
        p_list = remove_par(list(p))
        if not isinstance(p_list[1], list):
            p[0] = [p_list[1]]
        else:
            p[0] = p_list[1]
            if len(p_list) > 2:
                p[0].append(p_list[-1])
