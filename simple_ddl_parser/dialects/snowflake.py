class Snowflake:
    def p_clone(self, p):
        """clone : CLONE ID"""
        p_list = list(p)
        p[0] = {"clone": {"from": p_list[-1]}}

    def p_table_properties(self, p):
        """table_properties : ID ID ID"""
        p_list = list(p)
        p[0] = {p_list[-3]: p_list[-1]}

    def p_expression_cluster_by(self, p):
        """expr : expr CLUSTER BY LP pid RP"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["cluster_by"] = p_list[-2]
