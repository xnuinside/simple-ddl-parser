import pytest

from simple_ddl_parser import utils


@pytest.mark.parametrize(
    "expression, expected_result",
    [
        ([], []),
        (["("], []),
        ([")"], []),
        (["(", ")"], []),
        ([")", "("], []),
        (["(", "A"], ["A"]),
        (["A", ")"], ["A"]),
        (["(", "A", ")"], ["A"]),
        (["A", ")", ")"], ["A"]),
        (["(", "(", "A"], ["A"]),
        (["A", "B", "C"], ["A", "B", "C"]),
        (["A", "(", "(", "B", "C", "("], ["A", "B", "C"]),
        (["A", ")", "B", ")", "(", "C"], ["A", "B", "C"]),
        (["(", "A", ")", "B", "C", ")"], ["A", "B", "C"]),
        ([dict()], [dict()]),  # Edge case (unhashable types)
    ]
)
def test_remove_par(expression, expected_result):
    assert utils.remove_par(expression) == expected_result


@pytest.mark.parametrize(
    "expression, expected_result",
    [
        ("", ""),
        ("simple", "simple"),

        ("'pars_m_t'", "'\t'"),
        ("'pars_m_n'", "'\n'"),
        ("'pars_m_dq'", '"'),
        ("pars_m_single", "'"),

        ("STRING_'pars_m_t'STRING", "STRING_'\t'STRING"),
        ("STRING_'pars_m_n'STRING", "STRING_'\n'STRING"),
        ("STRING_'pars_m_dq'STRING", "STRING_\"STRING"),
        ("STRING_pars_m_singleSTRING", "STRING_'STRING"),

        ("pars_m_single pars_m_single", "' '"),
        ("'pars_m_t''pars_m_n'", "'\t''pars_m_n'"),  # determined by dict element order
    ]
)
def test_check_spec(expression, expected_result):
    assert utils.check_spec(expression) == expected_result


@pytest.mark.parametrize(
    "expression, expected_result",
    [
        (")", 0),
        (")()", 0),
        ("())", 2),
        ("()())", 4),
        ("", None),
        ("text", None),
        ("()", None),
        ("(balanced) (brackets)", None),
        ("(not)) (balanced) (brackets", 5)
    ]
)
def test_find_first_unpair_closed_par(expression, expected_result):
    assert utils.find_first_unpair_closed_par(expression) == expected_result
