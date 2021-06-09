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

from .transform import Transform
from .transform_util import TransformUtil
from ..expressions import (Expressions,
                           Operation)
from ..types.types import (IntegerType,
                           TypeID)


class Timestamps(Transform):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"

    EPOCH = datetime.datetime.utcfromtimestamp(0)
    HUMAN_FUNCS = {"year": lambda x: TransformUtil.human_year(x),
                   "month": lambda x: TransformUtil.human_month(x),
                   "day": lambda x: TransformUtil.human_day(x),
                   "hour": lambda x: TransformUtil.human_hour(x)}

    def __init__(self, granularity, name):
        if granularity not in (Timestamps.YEAR, Timestamps.MONTH, Timestamps.DAY, Timestamps.HOUR):
            raise RuntimeError("Invalid Granularity: %s" % granularity)

        self.granularity = granularity
        self.name = name

    def apply(self, value):
        apply_func = getattr(TransformUtil, "diff_{}".format(self.granularity))
        return apply_func(datetime.datetime.utcfromtimestamp(value / 1000000), Timestamps.EPOCH)

    def can_transform(self, type_var):
        return type_var.type_id == TypeID.TIMESTAMP

    def get_result_type(self, source_type):
        return IntegerType.get()

    def project(self, name, predicate):
        if predicate.op == Operation.NOT_NULL or predicate.op == Operation.IS_NULL:
            return Expressions.predicate(predicate.op, self.name)

    def project_strict(self, name, predicate):
        return None

    def to_human_string(self, value):
        if value is None:
            return "null"

        return Timestamps.HUMAN_FUNCS[self.granularity](value)

    def __str__(self):
        return self.name

    def dedup_name(self):
        return "time"

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        if other is None or not isinstance(other, Timestamps):
            return False

        return self.granularity == other.granularity and self.name == other.name
