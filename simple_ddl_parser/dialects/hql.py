from simple_ddl_parser.utils import check_spec


class HQL:
    def p_expression_location(self, p):
        """expr : expr LOCATION STRING"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["location"] = p_list[-1]

    def p_row_format(self, p):
        """row_format : ROW FORMAT SERDE
        | ROW FORMAT
        """
        p_list = list(p)
        p[0] = {"serde": p_list[-1] == "SERDE"}

    def p_expression_row_format(self, p):
        """expr : expr row_format ID
        | expr row_format STRING
        """
        p[0] = p[1]
        p_list = list(p)
        if p[2]["serde"]:
            format = {"serde": True, "java_class": p_list[-1]}
        else:
            format = check_spec(p_list[-1])

        p[0]["row_format"] = format

    def p_assigment(self, p):
        """assigment : ID ID ID"""
        p_list = list(p)
        p[0] = {p[1]: self.lexer.state.get(p_list[-1])}

    def p_expression_with_serdie(self, p):
        """expr : expr WITH SERDEPROPERTIES LP assigment RP"""
        p[0] = p[1]
        p_list = list(p)
        row_format = p[0]["row_format"]
        row_format["properties"] = p_list[-2]
        p[0]["row_format"] = row_format

    def p_expression_comment(self, p):
        """expr : expr ID STRING"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["comment"] = check_spec(p_list[-1])

    def p_expression_terminated_by(self, p):
        """expr : expr ID TERMINATED BY ID
        | expr ID TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0][f"{p[2].lower()}_terminated_by"] = check_spec(p_list[-1])

    def p_expression_map_keys_terminated_by(self, p):
        """expr : expr MAP KEYS TERMINATED BY ID
        | expr MAP KEYS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["map_keys_terminated_by"] = check_spec(p_list[-1])

    def p_expression_collection_terminated_by(self, p):
        """expr : expr COLLECTION ITEMS TERMINATED BY ID
        | expr COLLECTION ITEMS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["collection_items_terminated_by"] = check_spec(p_list[-1])

    def p_expression_stored_as(self, p):
        """expr : expr STORED AS ID"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["stored_as"] = p_list[-1]

    def p_expression_partitioned_by_hql(self, p):
        """expr : expr PARTITIONED BY LP pid_with_type RP"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["partitioned_by"] = p_list[-2]
