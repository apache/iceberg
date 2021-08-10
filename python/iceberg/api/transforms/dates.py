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

import datetime

from .projection_util import (fix_inclusive_time_projection,
                              fix_strict_time_projection,
                              project_transform_predicate,
                              truncate_integer,
                              truncate_integer_strict)
from .transform import Transform
from .transform_util import TransformUtil
from ..expressions import (Expressions, Operation)
from ..expressions.predicate import BoundPredicate
from ..types.type import (Type, TypeID)
from ..types.types import (DateType, IntegerType)


class Dates(Transform):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"

    EPOCH = datetime.datetime.utcfromtimestamp(0)
    SECONDS_IN_DAY = 86400

    HUMAN_FUNCS = {"year": lambda x: TransformUtil.human_year(x),
                   "month": lambda x: TransformUtil.human_month(x),
                   "day": lambda x: TransformUtil.human_day(x)}

    def __init__(self, granularity, name: str):
        super().__init__()
        if granularity not in (Dates.YEAR, Dates.MONTH, Dates.DAY):
            raise RuntimeError("Invalid Granularity: %s" % granularity)
        self._granularity = granularity
        self._name = name

    def apply(self, days):
        if days is None:
            return None

        if self._granularity == Dates.DAY:
            return days

        apply_func = getattr(TransformUtil, "diff_{}".format(self._granularity))
        if days >= 0:
            return apply_func(datetime.datetime.utcfromtimestamp(days * Dates.SECONDS_IN_DAY), Dates.EPOCH)
        else:
            return apply_func(datetime.datetime.utcfromtimestamp((days + 1) * Dates.SECONDS_IN_DAY), Dates.EPOCH) - 1

    def can_transform(self, another: Type):
        return another.type_id == TypeID.DATE

    def get_result_type(self, source_type):
        if self._granularity == Dates.DAY:
            return DateType.get()

        return IntegerType.get()

    # todo support preservesOrder and satisfiesOrderOf

    def project(self, name, predicate: BoundPredicate):
        from ..expressions.transform import BoundTransform
        if isinstance(predicate.term, BoundTransform):
            return project_transform_predicate(self, name, predicate)

        if predicate.is_unary_predicate:
            return Expressions.predicate(predicate.op, name)
        elif predicate.is_literal_predicate:
            projected = truncate_integer(name, predicate, self)
            if self._granularity != Dates.DAY:
                return fix_inclusive_time_projection(projected)
            return projected
        elif predicate.is_set_predicate and predicate.op == Operation.IN:
            raise NotImplementedError

        return None

    def project_strict(self, name, predicate):
        from ..expressions.transform import BoundTransform
        if isinstance(predicate.term, BoundTransform):
            return project_transform_predicate(self, name, predicate)

        if predicate.is_unary_predicate:
            return Expressions.predicate(predicate.op, name)
        elif predicate.is_literal_predicate:
            projected = truncate_integer_strict(name, predicate, self)
            if self._granularity != Dates.DAY:
                return fix_strict_time_projection(projected)
            return projected
        elif predicate.is_set_predicate and predicate.op == Operation.NOT_IN:
            raise NotImplementedError

        return None

    def to_human_string(self, value):
        if value is None:
            return "null"

        apply_func = Dates.HUMAN_FUNCS.get(self._granularity)
        if apply_func is not None:
            return apply_func(value)
        else:
            raise ValueError(f"Unsupported time unit: {self._granularity}")

    def __str__(self):
        return self._name

    def dedup_name(self):
        return "time"

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, Dates):
            return False

        return self._granularity == other._granularity and self._name == other._name
