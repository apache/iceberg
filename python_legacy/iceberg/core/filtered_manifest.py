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

from iceberg.api.expressions import Evaluator, Expressions, inclusive, InclusiveMetricsEvaluator

from .manifest_entry import Status


class FilteredManifest(object):

    def __init__(self, reader, part_filter, row_filter, columns, case_sensitive=True):
        if reader is None:
            raise RuntimeError("ManifestReader cannot be null")

        self.reader = reader
        self.part_filter = part_filter
        self.row_filter = row_filter
        self.columns = columns
        self.case_sensitive = case_sensitive

        self.lazy_evaluator = None
        self.lazy_metrics_evaluator = None

    def select(self, columns):
        return FilteredManifest(self.reader, self.part_filter, self.row_filter, columns, self.case_sensitive)

    def filter_partitions(self, expr):
        return FilteredManifest(self.reader,
                                Expressions.and_(self.part_filter, expr),
                                self.row_filter,
                                self.columns,
                                self.case_sensitive)

    def filter_rows(self, expr):
        projected = inclusive(self.reader.spec).project(expr)
        return FilteredManifest(self.reader,
                                Expressions.and_(self.part_filter, projected),
                                Expressions.and_(self.row_filter, expr),
                                self.columns, self.case_sensitive)

    def all_entries(self):
        if self.row_filter is not None and self.row_filter != Expressions.always_true() \
                or self.part_filter is not None and self. part_filter != Expressions.always_true():
            evaluator = self.evaluator()
            metrics_evaluator = self.metrics_evaluator()
            return [entry for entry in self.reader.entries(self.columns)
                    if entry is not None
                    and evaluator.eval(entry.file.partition())
                    and metrics_evaluator.eval(entry.file)]
        else:
            return self.reader.entries(self.columns)

    def live_entries(self):
        if self.row_filter is not None and self.row_filter != Expressions.always_true() \
                or self.part_filter is not None and self. part_filter != Expressions.always_true():
            evaluator = self.evaluator()
            metrics_evaluator = self.metrics_evaluator()
            return [entry for entry in self.reader.entries(self.columns)
                    if entry is not None
                    and entry.status != Status.DELETED
                    and evaluator.eval(entry.file.partition())
                    and metrics_evaluator.eval(entry.file)]
        else:

            return [entry for entry in self.reader.entries(self.columns)
                    if entry is not None and entry.status != Status.DELETED]

    def iterator(self):
        if self.row_filter is not None and self.row_filter != Expressions.always_true() \
                or self.part_filter is not None and self.part_filter != Expressions.always_true():
            evaluator = self.evaluator()
            metrics_evaluator = self.metrics_evaluator()

            return (input.copy() for input in self.reader.iterator(self.part_filter, self.columns)
                    if input is not None
                    and evaluator.eval(input.partition())
                    and metrics_evaluator.eval(input))
        else:
            return (entry.copy() for entry in self.reader.iterator(self.part_filter, self.columns))

    def evaluator(self):
        if self.lazy_evaluator is None:
            if self.part_filter is not None:
                self.lazy_evaluator = Evaluator(self.reader.spec.partition_type(),
                                                self.part_filter,
                                                self.case_sensitive)
            else:
                self.lazy_evaluator = Evaluator(self.reader.spec.partition_type(),
                                                Expressions.always_true(),
                                                self.case_sensitive)

        return self.lazy_evaluator

    def metrics_evaluator(self):
        if self.lazy_metrics_evaluator is None:
            if self.row_filter is not None:
                self.lazy_metrics_evaluator = InclusiveMetricsEvaluator(self.reader.spec.schema,
                                                                        self.row_filter, self.case_sensitive)
            else:
                self.lazy_metrics_evaluator = InclusiveMetricsEvaluator(self.reader.spec.schema,
                                                                        Expressions.always_true(),
                                                                        self.case_sensitive)

        return self.lazy_metrics_evaluator
