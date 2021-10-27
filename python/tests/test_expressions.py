import pytest

from iceberg import expressions
from iceberg.exceptions import OperationError
from iceberg.expressions import Operation  # noqa: F401


def test_readable_str_of_operations():
    assert str(expressions.Operation.TRUE) == "TRUE"
    assert str(expressions.Operation.FALSE) == "FALSE"
    assert str(expressions.Operation.IS_NULL) == "IS_NULL"
    assert str(expressions.Operation.NOT_NULL) == "NOT_NULL"
    assert str(expressions.Operation.IS_NAN) == "IS_NAN"
    assert str(expressions.Operation.NOT_NAN) == "NOT_NAN"
    assert str(expressions.Operation.LT) == "LT"
    assert str(expressions.Operation.LT_EQ) == "LT_EQ"
    assert str(expressions.Operation.GT) == "GT"
    assert str(expressions.Operation.GT_EQ) == "GT_EQ"
    assert str(expressions.Operation.EQ) == "EQ"
    assert str(expressions.Operation.NOT_EQ) == "NOT_EQ"
    assert str(expressions.Operation.IN) == "IN"
    assert str(expressions.Operation.NOT_IN) == "NOT_IN"
    assert str(expressions.Operation.NOT) == "NOT"
    assert str(expressions.Operation.AND) == "AND"
    assert str(expressions.Operation.OR) == "OR"


@pytest.mark.parametrize(
    "input_type",
    [
        expressions.Operation.TRUE,
        expressions.Operation.FALSE,
        expressions.Operation.IS_NULL,
        expressions.Operation.NOT_NULL,
        expressions.Operation.IS_NAN,
        expressions.Operation.NOT_NAN,
        expressions.Operation.LT,
        expressions.Operation.LT_EQ,
        expressions.Operation.GT,
        expressions.Operation.GT_EQ,
        expressions.Operation.EQ,
        expressions.Operation.NOT_EQ,
        expressions.Operation.IN,
        expressions.Operation.NOT_IN,
        expressions.Operation.NOT,
        expressions.Operation.AND,
        expressions.Operation.OR,
    ],
)
def test_official_representation_of_operations(input_type):
    assert input_type == eval(repr(input_type))


@pytest.mark.parametrize(
    "operation,opposite_operation",
    [
        (expressions.Operation.TRUE, expressions.Operation.FALSE),
        (expressions.Operation.FALSE, expressions.Operation.TRUE),
        (expressions.Operation.IS_NULL, expressions.Operation.NOT_NULL),
        (expressions.Operation.NOT_NULL, expressions.Operation.IS_NULL),
        (expressions.Operation.IS_NAN, expressions.Operation.NOT_NAN),
        (expressions.Operation.NOT_NAN, expressions.Operation.IS_NAN),
        (expressions.Operation.LT, expressions.Operation.GT_EQ),
        (expressions.Operation.LT_EQ, expressions.Operation.GT),
        (expressions.Operation.GT, expressions.Operation.LT_EQ),
        (expressions.Operation.GT_EQ, expressions.Operation.LT),
        (expressions.Operation.EQ, expressions.Operation.NOT_EQ),
        (expressions.Operation.NOT_EQ, expressions.Operation.EQ),
        (expressions.Operation.IN, expressions.Operation.NOT_IN),
        (expressions.Operation.NOT_IN, expressions.Operation.IN),
    ],
)
def test_negation_of_operations(operation, opposite_operation):
    assert -operation == opposite_operation


@pytest.mark.parametrize(
    "operation",
    [
        expressions.Operation.NOT,
        expressions.Operation.AND,
        expressions.Operation.OR,
    ],
)
def test_raise_on_no_negation_for_operation(operation):
    with pytest.raises(OperationError) as exc_info:
        -operation

    assert str(exc_info.value) == f"No negation defined for operation {operation}"


@pytest.mark.parametrize(
    "operation,reversed_operation",
    [
        (expressions.Operation.LT, expressions.Operation.GT),
        (expressions.Operation.LT_EQ, expressions.Operation.GT_EQ),
        (expressions.Operation.GT, expressions.Operation.LT),
        (expressions.Operation.GT_EQ, expressions.Operation.LT_EQ),
        (expressions.Operation.EQ, expressions.Operation.EQ),
        (expressions.Operation.NOT_EQ, expressions.Operation.NOT_EQ),
        (expressions.Operation.AND, expressions.Operation.AND),
        (expressions.Operation.OR, expressions.Operation.OR),
    ],
)
def test_reversing_operations(operation, reversed_operation):
    assert reversed(operation) == reversed_operation


@pytest.mark.parametrize(
    "operation",
    [
        expressions.Operation.TRUE,
        expressions.Operation.FALSE,
        expressions.Operation.IS_NULL,
        expressions.Operation.NOT_NULL,
        expressions.Operation.IS_NAN,
        expressions.Operation.NOT_NAN,
    ],
)
def test_raise_on_no_reverse_of_operation(operation):
    with pytest.raises(OperationError) as exc_info:
        reversed(operation)

    assert str(exc_info.value) == f"No left-right flip for operation {operation}"
