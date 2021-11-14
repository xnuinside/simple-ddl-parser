class BigQuery:
    def p_expression_options(self, p):
        """expr : expr options """
        print(list(p))
        p[0] = p[1]
        p[1].update(p[2])

    def p_options(self, p):
        """options : OPTIONS LP id_equals RP """
        print(list(p))
        p[0] = {"options": p[3]}
