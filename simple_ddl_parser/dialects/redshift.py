class Redshift:
    def p_expression_distkey(self, p):
        """expr : expr ID LP ID RP"""
        p_list = list(p)
        p[1].update({"distkey": p_list[-2]})
        p[0] = p[1]

    def p_encode(self, p):
        """encode : ENCODE ID"""
        p_list = list(p)
        p[0] = {"encode": p_list[-1]}

    def p_expression_diststyle(self, p):
        """expr : expr ID ID
        | expr ID KEY
        """
        p_list = list(p)
        p[1].update({p_list[-2]: p_list[-1]})
        p[0] = p[1]

    def p_expression_sortkey(self, p):
        """expr : expr ID ID LP pid RP"""
        p_list = list(p)
        p[1].update({"sortkey": {"type": p_list[2], "keys": p_list[-2]}})
        p[0] = p[1]
