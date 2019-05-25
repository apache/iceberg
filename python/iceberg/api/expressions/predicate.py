# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from iceberg.exceptions import ValidationException

from .expression import (Expression,
                         FALSE,
                         Operation,
                         TRUE)
from .literals import (Literal,
                       Literals)
from .reference import BoundReference


class Predicate(Expression):

    def __init__(self, op, ref, lit):
        self.op = op
        self.ref = ref
        self.lit = lit

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, Predicate):
            return False

        return self.op == other.op and self.ref == other.ref and self.lit == other.lit

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "Predicate({},{},{})".format(self.op, self.ref, self.lit)

    def __str__(self):
        if self.op == Operation.IS_NULL:
            return "is_null({})".format(self.ref)
        elif self.op == Operation.NOT_NULL:
            return "not_null({})".format(self.ref)
        elif self.op == Operation.LT:
            return "less_than({})".format(self.ref)
        elif self.op == Operation.LT_EQ:
            return "less_than_equal({})".format(self.ref)
        elif self.op == Operation.GT:
            return "greater_than({})".format(self.ref)
        elif self.op == Operation.GT_EQ:
            return "greater_than_equal({})".format(self.ref)
        elif self.op == Operation.EQ:
            return "equal({})".format(self.ref)
        elif self.op == Operation.NOT_EQ:
            return "not_equal({})".format(self.ref)
        else:
            return "invalid predicate: operation = {}".format(self.op)


class BoundPredicate(Predicate):

    def __init__(self, op, ref, lit=None):
        super(BoundPredicate, self).__init__(op, ref, lit)

    def negate(self):
        return BoundPredicate(self.op.negate(), self.ref, self.lit)


class UnboundPredicate(Predicate):

    def __init__(self, op, named_ref, value=None, lit=None):
        if value is None and lit is None:
            super(UnboundPredicate, self).__init__(op, named_ref, None)
        if isinstance(value, Literal):
            lit = value
            value = None
        if value is not None:
            super(UnboundPredicate, self).__init__(op, named_ref, Literals.from_(value))
        elif lit is not None:
            super(UnboundPredicate, self).__init__(op, named_ref, lit)

    def negate(self):
        return UnboundPredicate(self.op.negate(), self.ref, self.lit)

    def bind(self, struct, case_sensitive=True):  # noqa: C901
        if case_sensitive:
            field = struct.field(self.ref.name)
        else:
            field = struct.case_insensitive_field(self.ref.name.lower())

        ValidationException.check(field is not None,
                                  "Cannot find field '%s' in struct %s", (self.ref.name, struct))

        if self.lit is None:
            if self.op == Operation.IS_NULL:
                if field.is_required:
                    return FALSE
                return BoundPredicate(Operation.IS_NULL, BoundReference(struct, field.field_id))
            elif self.op == Operation.NOT_NULL:
                if field.is_required:
                    return TRUE
                return BoundPredicate(Operation.NOT_NULL, BoundReference(struct, field.field_id))
            else:
                raise ValidationException("Operation must be IS_NULL or NOT_NULL", None)

        literal = self.lit.to(field.type)
        if literal is None:
            raise ValidationException("Invalid value for comparison inclusive type %s: %s (%s)",
                                      (field.type, self.lit.value, type(self.lit.value)))
        elif literal == Literals.above_max():
            if self.op in (Operation.LT,
                           Operation.LT_EQ,
                           Operation.NOT_EQ):
                return TRUE
            elif self.op in (Operation.GT,
                             Operation.GT_EQ,
                             Operation.EQ):
                return FALSE

        elif literal == Literals.below_min():
            if self.op in (Operation.LT,
                           Operation.LT_EQ,
                           Operation.NOT_EQ):
                return FALSE
            elif self.op in (Operation.GT,
                             Operation.GT_EQ,
                             Operation.EQ):
                return TRUE

        return BoundPredicate(self.op, BoundReference(struct, field.field_id), literal)
