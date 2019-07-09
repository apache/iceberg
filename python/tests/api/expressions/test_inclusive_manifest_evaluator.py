
from iceberg.api.expressions import Expressions, InclusiveManifestEvaluator
from iceberg.exceptions import ValidationException
import pytest


@pytest.mark.parametrize("expression,expected", [
    (Expressions.not_null("all_nulls"), False),
    (Expressions.not_null("some_nulls"), True),
    (Expressions.not_null("no_nulls"), True)])
def test_all_nulls(inc_man_spec, inc_man_file, expression, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, expression).eval(inc_man_file) == expected


@pytest.mark.parametrize("expression,expected", [
    (Expressions.is_null("all_nulls"), True),
    (Expressions.is_null("some_nulls"), True),
    (Expressions.is_null("no_nulls"), False)])
def test_no_nulls(inc_man_spec, inc_man_file, expression, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, expression).eval(inc_man_file) == expected


def test_missing_column(inc_man_spec, inc_man_file):
    with pytest.raises(ValidationException):
        InclusiveManifestEvaluator(inc_man_spec, Expressions.less_than("missing", 5)).eval(inc_man_file)


@pytest.mark.parametrize("expression", [
    Expressions.less_than("id", 5),
    Expressions.less_than_or_equal("id", 30),
    Expressions.equal("id", 70),
    Expressions.greater_than("id", 78),
    Expressions.greater_than_or_equal("id", 90),
    Expressions.not_equal("id", 101),
    Expressions.less_than_or_equal("id", 30),
    Expressions.is_null("id"),
    Expressions.not_null("id")])
def test_missing_stats(inc_man_spec, inc_man_file_ns, expression):
    assert InclusiveManifestEvaluator(inc_man_spec, expression).eval(inc_man_file_ns)


@pytest.mark.parametrize("expression, expected", [
    (Expressions.less_than("id", 5), True),
    (Expressions.greater_than("id", 5), False)])
def test_not(inc_man_spec, inc_man_file, expression, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, Expressions.not_(expression)).eval(inc_man_file) == expected


@pytest.mark.parametrize("expr1, expr2, expected", [
    (Expressions.less_than("id", 5), Expressions.greater_than_or_equal("id", 0), False),
    (Expressions.greater_than("id", 5), Expressions.less_than_or_equal("id", 30), True)])
def test_and(inc_man_spec, inc_man_file, expr1, expr2, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, Expressions.and_(expr1, expr2)).eval(inc_man_file) == expected


@pytest.mark.parametrize("expr1, expr2, expected", [
    (Expressions.less_than("id", 5), Expressions.greater_than_or_equal("id", 80), False),
    (Expressions.less_than("id", 5), Expressions.greater_than_or_equal("id", 60), True)])
def test_or(inc_man_spec, inc_man_file, expr1, expr2, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, Expressions.or_(expr1, expr2)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, False),
    (30, False),
    (31, True),
    (79, True)])
def test_int_lt(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, Expressions.less_than("id", val)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, False),
    (29, False),
    (30, True),
    (79, True)])
def test_int_lt_eq(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec,
                                      Expressions.less_than_or_equal("id", val)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (85, False),
    (79, False),
    (78, True),
    (75, True)])
def test_int_gt(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec, Expressions.greater_than("id", val)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (85, False),
    (80, False),
    (79, True),
    (75, True)])
def test_int_gt_eq(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec,
                                      Expressions.greater_than_or_equal("id", val)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, False),
    (29, False),
    (30, True),
    (75, True),
    (79, True),
    (80, False),
    (85, False)])
def test_int_eq(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec,
                                      Expressions.equal("id", val)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, True),
    (29, True),
    (30, True),
    (75, True),
    (79, True),
    (80, True),
    (85, True)])
def test_int_not_eq(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec,
                                      Expressions.not_equal("id", val)).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, True),
    (29, True),
    (30, True),
    (75, True),
    (79, True),
    (80, True),
    (85, True)])
def test_int_not_eq_rewritten(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec,
                                      Expressions.not_(Expressions.equal("id", val))).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, True),
    (29, True),
    (30, True),
    (75, True),
    (79, True),
    (80, True),
    (85, True)])
def test_case_insensitive_int_not_eq_rewritten(inc_man_spec, inc_man_file, val, expected):
    assert InclusiveManifestEvaluator(inc_man_spec,
                                      Expressions.not_(Expressions.equal("ID", val)),
                                      case_sensitive=False).eval(inc_man_file) == expected


@pytest.mark.parametrize("val, expected", [
    (5, True),
    (29, True),
    (30, True),
    (75, True),
    (79, True),
    (80, True),
    (85, True)])
def test_case_sensitive_int_not_eq_rewritten(inc_man_spec, inc_man_file, val, expected):
    with pytest.raises(ValidationException):
        InclusiveManifestEvaluator(inc_man_spec,
                                   Expressions.not_(Expressions.equal("ID", val)),
                                   case_sensitive=True).eval(inc_man_file) == expected
