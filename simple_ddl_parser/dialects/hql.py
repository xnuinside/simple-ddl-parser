class HQL:
    def p_expression_location(self, p):
        """expr : expr LOCATION STRING"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["location"] = p_list[-1]

    def p_expression_row_format(self, p):
        """expr : expr ROW FORMAT ID
        | expr ROW FORMAT STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["row_format"] = p_list[-1]

    def p_expression_fields_terminated_by(self, p):
        """expr : expr FIELDS TERMINATED BY ID
        | expr FIELDS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["fields_terminated_by"] = p_list[-1]

    def p_expression_map_keys_terminated_by(self, p):
        """expr : expr MAP KEYS TERMINATED BY ID
        | expr MAP KEYS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["map_keys_terminated_by"] = p_list[-1]

    def p_expression_collection_terminated_by(self, p):
        """expr : expr COLLECTION ITEMS TERMINATED BY ID
        | expr COLLECTION ITEMS TERMINATED BY STRING
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]["collection_items_terminated_by"] = p_list[-1]

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
