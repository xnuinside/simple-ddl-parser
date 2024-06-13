import pytest

import utils


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


@pytest.mark.parametrize(
    "expression, expected_result",
    [
        ([], []),
        (["("], []),
        ([")"], []),
        (["(", ")"], []),
        (["(", ")"], []),
        (["(", "A", ")"], ["A"]),
        (["A", ")", ")"], ["A"]),
        (["(", "(", "A"], ["A"]),
        (["A", "(", "(", "B", "C", "("], ["A", "B", "C"]),
        (["A", ")", "B", ")", "(", "C"], ["A", "B", "C"]),

    ]
)
def test_remove_par(expression, expected_result):
    assert utils.remove_par(expression) == expected_result
