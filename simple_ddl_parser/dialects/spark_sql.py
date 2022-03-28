class SparkSQL:
    def p_expression_using(self, p):
        """expr : expr using"""
        p[0] = p[1]
        p[1].update(p[2])

    def p_using(self, p):
        """using : USING id"""
        p_list = list(p)
        p[0] = {"using": p_list[-1]}
