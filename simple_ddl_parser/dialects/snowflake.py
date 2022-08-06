from simple_ddl_parser.utils import remove_par


class Snowflake:
    def p_clone(self, p):
        """clone : CLONE id"""
        p_list = list(p)
        p[0] = {"clone": {"from": p_list[-1]}}

    def p_expression_cluster_by(self, p):
        """expr : expr CLUSTER BY LP pid RP
        | expr CLUSTER BY pid
        """
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["cluster_by"] = p_list[-1]

    def p_table_comment(self, p):
        """expr : expr option_comment
        """
        p[0] = p[1]
        if p[2]:
            p[0].update(p[2])

    def p_option_comment(self, p):
        """option_comment : ID STRING
        | ID DQ_STRING
        | COMMENT ID STRING
        | COMMENT ID DQ_STRING
        """
        p_list = remove_par(list(p))
        if "comment" in p[1].lower():
            p[0] = {"comment": p_list[-1]}
