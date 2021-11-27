class BigQuery:
    def p_expression_options(self, p):
        """expr : expr multiple_options"""
        p[0] = p[1]
        p[1].update(p[2])

    def p_multiple_options(self, p):
        """multiple_options : options
        | multiple_options options
        """
        if len(p) > 2:
            p[1]["options"].extend(p[2]["options"])
            p[0] = p[1]
        else:
            p[0] = p[1]

    def p_options(self, p):
        """options : OPTIONS LP id_equals RP"""
        p_list = list(p)
        if not isinstance(p[1], dict):
            p[0] = {"options": p[3]}
        else:
            p[0] = p[1]
            if len(p) == 4:
                p[0]["options"].append(p_list[-1][0])
