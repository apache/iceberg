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

import threading

from .binder import Binder
from .expressions import Expressions, ExpressionVisitors
from .projections import inclusive
from ..types import Conversions

ROWS_MIGHT_MATCH = True
ROWS_CANNOT_MATCH = False


class InclusiveManifestEvaluator(object):

    def __init__(self, spec, row_filter, case_sensitive=True):
        self.struct = spec.partition_type()
        self.expr = Binder.bind(self.struct,
                                Expressions.rewrite_not(inclusive(spec, case_sensitive=case_sensitive)
                                                        .project(row_filter)),
                                case_sensitive=case_sensitive)
        self.thread_local_data = threading.local()

    def _visitor(self):
        if not hasattr(self.thread_local_data, "visitors"):
            self.thread_local_data.visitors = ManifestEvalVisitor(self.expr)

        return self.thread_local_data.visitors

    def eval(self, manifest):
        return self._visitor().eval(manifest)


class ManifestEvalVisitor(ExpressionVisitors.BoundExpressionVisitor):

    def __init__(self, expr):
        self.expr = expr
        self.stats = None

    def eval(self, manifest):
        self.stats = manifest.partitions
        if self.stats is None:
            return ROWS_MIGHT_MATCH

        return ExpressionVisitors.visit(self.expr, self)

    def always_true(self):
        return ROWS_MIGHT_MATCH

    def always_false(self):
        return ROWS_CANNOT_MATCH

    def not_(self, result):
        return not result

    def and_(self, left_result, right_result):
        return left_result and right_result

    def or_(self, left_result, right_result):
        return left_result or right_result

    def is_null(self, ref):
        if not self.stats[ref.pos].contains_null():
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def not_null(self, ref):
        lower_bound = self.stats[ref.pos].lower_bound()
        if lower_bound is None:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def lt(self, ref, lit):
        lower_bound = self.stats[ref.pos].lower_bound()
        if lower_bound is None:
            return ROWS_CANNOT_MATCH

        lower = Conversions.from_byte_buffer(ref.type, lower_bound)

        if lower >= lit.value:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def lt_eq(self, ref, lit):
        lower_bound = self.stats[ref.pos].lower_bound()
        if lower_bound is None:
            return ROWS_CANNOT_MATCH

        lower = Conversions.from_byte_buffer(ref.type, lower_bound)

        if lower > lit.value:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def gt(self, ref, lit):
        upper_bound = self.stats[ref.pos].upper_bound()
        if upper_bound is None:
            return ROWS_CANNOT_MATCH

        upper = Conversions.from_byte_buffer(ref.type, upper_bound)

        if upper <= lit.value:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def gt_eq(self, ref, lit):
        upper_bound = self.stats[ref.pos].upper_bound()
        if upper_bound is None:
            return ROWS_CANNOT_MATCH

        upper = Conversions.from_byte_buffer(ref.type, upper_bound)

        if upper < lit.value:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def eq(self, ref, lit):
        field_stats = self.stats[ref.pos]
        if field_stats.lower_bound() is None:
            return ROWS_CANNOT_MATCH

        lower = Conversions.from_byte_buffer(ref.type, field_stats.lower_bound())
        if lower > lit.value:
            return ROWS_CANNOT_MATCH

        upper = Conversions.from_byte_buffer(ref.type, field_stats.upper_bound())

        if upper < lit.value:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def not_eq(self, ref, lit):
        return ROWS_MIGHT_MATCH

    def in_(self, ref, lit):
        return ROWS_MIGHT_MATCH

    def not_in(self, ref, lit):
        return ROWS_MIGHT_MATCH
