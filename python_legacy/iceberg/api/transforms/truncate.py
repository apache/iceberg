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

from decimal import Decimal

from .projection_util import ProjectionUtil
from .transform import Transform
from .transform_util import TransformUtil
from ..expressions import (Expressions,
                           Operation)
from ..types import TypeID


class Truncate(Transform):

    @staticmethod
    def get(type_var, width):
        if type_var.type_id == TypeID.INTEGER:
            return TruncateInteger(width)
        elif type_var.type_id == TypeID.LONG:
            return TruncateLong(width)
        elif type_var.type_id == TypeID.DECIMAL:
            return TruncateDecimal(width)
        elif type_var.type_id == TypeID.STRING:
            return TruncateString(width)

    def __init__(self):
        raise NotImplementedError()

    def apply(self, value):
        raise NotImplementedError()

    def can_transform(self, type_var):
        raise NotImplementedError()

    def get_result_type(self, source_type):
        return source_type

    def project(self, name, predicate):
        raise NotImplementedError()

    def project_strict(self, name, predicate):
        raise NotImplementedError()


class TruncateInteger(Truncate):

    def __init__(self, width):
        self.W = width

    def apply(self, value):
        return value - (((value % self.W) + self.W) % self.W)

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.INTEGER

    def project(self, name, predicate):
        if predicate.op == Operation.NOT_NULL or predicate.op == Operation.IS_NULL:
            return Expressions.predicate(predicate.op, name)

        return ProjectionUtil.truncate_integer(name, predicate, self)

    def project_strict(self, name, predicate):
        if predicate.op == Operation.LT:
            _in = predicate.lit.value - 1
            _out = predicate.lit.value
            in_image = self.apply(_in)
            out_image = self.apply(_out)
            if in_image != out_image:
                return Expressions.predicate(Operation.LT_EQ, name, in_image)
            else:
                return Expressions.predicate(Operation.LT, name, in_image)
        elif predicate.op == Operation.LT_EQ:
            _in = predicate.lit.value
            _out = predicate.lit.value + 1
            in_image = self.apply(_in)
            out_image = self.apply(_out)
            if in_image != out_image:
                return Expressions.predicate(Operation.LT_EQ, name, in_image)
            else:
                return Expressions.predicate(Operation.LT, name, in_image)

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, TruncateInteger):
            return False

        return self.W == other.W

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return TruncateInteger.__class__, self.W

    def __str__(self):
        return "truncate[%s]" % self.W


class TruncateLong(Truncate):

    def __init__(self, width):
        self.W = width

    def apply(self, value):
        return value - (((value % self.W) + self.W) % self.W)

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.LONG

    def project(self, name, predicate):
        if predicate.op == Operation.NOT_NULL or predicate.op == Operation.IS_NULL:
            return Expressions.predicate(predicate.op, name)

        return ProjectionUtil.truncate_long(name, predicate, self)

    def project_strict(self, name, predicate):
        return None

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, TruncateLong):
            return False

        return self.W == other.W

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return TruncateLong.__class__, self.W

    def __str__(self):
        return "truncate[%s]" % self.W


class TruncateDecimal(Truncate):

    def __init__(self, unscaled_width):
        self.unscaled_width = unscaled_width

    def apply(self, value):
        unscaled_value = TransformUtil.unscale_decimal(value)
        applied_value = unscaled_value - (((unscaled_value % self.unscaled_width) + self.unscaled_width) % self.unscaled_width)
        return Decimal("{}e{}".format(applied_value, value.as_tuple().exponent))

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.DECIMAL

    def project(self, name, predicate):
        if predicate.op == Operation.NOT_NULL or predicate.op == Operation.IS_NULL:
            return Expressions.predicate(predicate.op, name)

        return ProjectionUtil.truncate_decimal(name, predicate, self)

    def project_strict(self, name, predicate):
        return None

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, TruncateDecimal):
            return False

        return self.unscaled_width == other.unscaled_width

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return TruncateDecimal.__class__, self.unscaled_width

    def __str__(self):
        return "truncate[%s]" % self.unscaled_width


class TruncateString(Truncate):
    def __init__(self, length):
        self.L = length

    def apply(self, value):
        return value[0:min(self.L, len(value))]

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.STRING

    def project(self, name, predicate):
        if predicate.op == Operation.NOT_NULL or predicate.op == Operation.IS_NULL:
            return Expressions.predicate(predicate.op, name)

        return ProjectionUtil.truncate_array(name, predicate, self)

    def project_strict(self, name, predicate):
        return None

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, TruncateString):
            return False

        return self.L == other.L

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return TruncateString.__class__, self.L

    def __str__(self):
        return "truncate[%s]" % self.L
