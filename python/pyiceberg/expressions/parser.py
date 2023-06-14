#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
from decimal import Decimal

from pyparsing import (
    CaselessKeyword,
    Group,
    ParserElement,
    ParseResults,
    Suppress,
    Word,
    alphanums,
    alphas,
    delimited_list,
    infix_notation,
    one_of,
    opAssoc,
    sgl_quoted_string,
)
from pyparsing.common import pyparsing_common as common

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    NotStartsWith,
    Or,
    Reference,
    StartsWith,
)
from pyiceberg.expressions.literals import (
    DecimalLiteral,
    Literal,
    LongLiteral,
    StringLiteral,
)
from pyiceberg.typedef import L

ParserElement.enablePackrat()

AND = CaselessKeyword("and")
OR = CaselessKeyword("or")
NOT = CaselessKeyword("not")
IS = CaselessKeyword("is")
IN = CaselessKeyword("in")
NULL = CaselessKeyword("null")
NAN = CaselessKeyword("nan")
LIKE = CaselessKeyword("like")

identifier = Word(alphas, alphanums + "_$").set_results_name("identifier")
column = delimited_list(identifier, delim=".", combine=True).set_results_name("column")


@column.set_parse_action
def _(result: ParseResults) -> Reference:
    return Reference(result.column[0])


boolean = one_of(["true", "false"], caseless=True).set_results_name("boolean")
string = sgl_quoted_string.set_results_name("raw_quoted_string")
decimal = common.real().set_results_name("decimal")
integer = common.signed_integer().set_results_name("integer")
literal = Group(string | decimal | integer).set_results_name("literal")
literal_set = Group(delimited_list(string) | delimited_list(decimal) | delimited_list(integer)).set_results_name("literal_set")


@boolean.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    if "true" == result.boolean.lower():
        return AlwaysTrue()
    else:
        return AlwaysFalse()


@string.set_parse_action
def _(result: ParseResults) -> Literal[str]:
    return StringLiteral(result.raw_quoted_string[1:-1].replace("''", "'"))


@decimal.set_parse_action
def _(result: ParseResults) -> Literal[Decimal]:
    return DecimalLiteral(Decimal(result.decimal))


@integer.set_parse_action
def _(result: ParseResults) -> Literal[int]:
    return LongLiteral(int(result.integer))


@literal.set_parse_action
def _(result: ParseResults) -> Literal[L]:
    return result[0][0]


@literal_set.set_parse_action
def _(result: ParseResults) -> Literal[L]:
    return result[0]


comparison_op = one_of(["<", "<=", ">", ">=", "=", "==", "!=", "<>"], caseless=True).set_results_name("op")
left_ref = column + comparison_op + literal
right_ref = literal + comparison_op + column
comparison = left_ref | right_ref


@left_ref.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    if result.op == "<":
        return LessThan(result.column, result.literal)
    elif result.op == "<=":
        return LessThanOrEqual(result.column, result.literal)
    elif result.op == ">":
        return GreaterThan(result.column, result.literal)
    elif result.op == ">=":
        return GreaterThanOrEqual(result.column, result.literal)
    if result.op in ("=", "=="):
        return EqualTo(result.column, result.literal)
    if result.op in ("!=", "<>"):
        return NotEqualTo(result.column, result.literal)
    raise ValueError(f"Unsupported operation type: {result.op}")


@right_ref.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    if result.op == "<":
        return GreaterThan(result.column, result.literal)
    elif result.op == "<=":
        return GreaterThanOrEqual(result.column, result.literal)
    elif result.op == ">":
        return LessThan(result.column, result.literal)
    elif result.op == ">=":
        return LessThanOrEqual(result.column, result.literal)
    elif result.op in ("=", "=="):
        return EqualTo(result.column, result.literal)
    elif result.op in ("!=", "<>"):
        return NotEqualTo(result.column, result.literal)
    raise ValueError(f"Unsupported operation type: {result.op}")


is_null = column + IS + NULL
not_null = column + IS + NOT + NULL
null_check = is_null | not_null


@is_null.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return IsNull(result.column)


@not_null.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return NotNull(result.column)


is_nan = column + IS + NAN
not_nan = column + IS + NOT + NAN
nan_check = is_nan | not_nan


@is_nan.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return IsNaN(result.column)


@not_nan.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return NotNaN(result.column)


is_in = column + IN + "(" + literal_set + ")"
not_in = column + NOT + IN + "(" + literal_set + ")"
in_check = is_in | not_in


@is_in.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return In(result.column, result.literal_set)


@not_in.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return NotIn(result.column, result.literal_set)


starts_with = column + LIKE + string
not_starts_with = column + NOT + LIKE + string
starts_check = starts_with | not_starts_with


@starts_with.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return StartsWith(result.column, result.raw_quoted_string)


@not_starts_with.set_parse_action
def _(result: ParseResults) -> BooleanExpression:
    return NotStartsWith(result.column, result.raw_quoted_string)


predicate = (comparison | in_check | null_check | nan_check | starts_check | boolean).set_results_name("predicate")


def handle_not(result: ParseResults) -> Not:
    return Not(result[0][0])


def handle_and(result: ParseResults) -> And:
    return And(result[0][0], result[0][1])


def handle_or(result: ParseResults) -> Or:
    return Or(result[0][0], result[0][1])


boolean_expression = infix_notation(
    predicate,
    [
        (Suppress(NOT), 1, opAssoc.RIGHT, handle_not),
        (Suppress(AND), 2, opAssoc.LEFT, handle_and),
        (Suppress(OR), 2, opAssoc.LEFT, handle_or),
    ],
).set_name("expr")


def parse(expr: str) -> BooleanExpression:
    """Parses a boolean expression."""
    return boolean_expression.parse_string(expr)[0]
