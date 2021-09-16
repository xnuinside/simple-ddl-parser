class BigQuery:
    def p_options(self, p):
        """expr : expr LOCATION STRING"""
        p[0] = p[1]
        p_list = list(p)
        p[0]["location"] = p_list[-1]
