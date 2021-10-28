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
from urllib import parse
from collections import defaultdict
from typing import List, Optional

from iceberg.partition_field import PartitionField
from iceberg.schema import Schema
from iceberg.types import Type, NestedField
from iceberg.validation_exception import ValidationException

PARTITION_DATA_ID_START = 1000


class PartitionSpec(object):
    fields_by_source_id: defaultdict[list] = None
    field_list: List[PartitionField] = None

    def __init__(
        self,
        schema: Schema,
        spec_id: int,
        part_fields: List[PartitionField],
        last_assigned_field_id: int,
    ):
        self._schema = schema
        self._spec_id = spec_id
        self._fields = [part_field for part_field in part_fields]
        self._last_assigned_field_id = last_assigned_field_id

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def spec_id(self) -> int:
        return self._spec_id

    @property
    def fields(self) -> List[PartitionField]:
        return self._fields

    @property
    def last_assigned_field_id(self) -> int:
        return self._last_assigned_field_id

    def is_partitioned(self):
        return len(self.fields) < 1

    def _generate_fields_by_source_id(self):
        if self.fields_by_source_id is None:
            fields_source_to_field_dict = defaultdict(list)
            for field in self.fields:
                fields_source_to_field_dict[field.source_id] = [field]
            return fields_source_to_field_dict
        return None

    def get_fields_by_source_id(self, field_id: int) -> List[PartitionField]:
        return self._generate_fields_by_source_id().get(field_id, None)

    def __eq__(self, other):
        if isinstance(other, PartitionSpec):
            if self.spec_id != other.spec_id:
                return False
            return self.fields == other.fields
        return False

    def __str__(self):
        partition_spec_str = "["
        for field in self.fields:
            partition_spec_str += "\n"
            partition_spec_str += " " + field
        if len(self.fields) > 0:
            partition_spec_str += "\n"
        partition_spec_str += "]"
        return partition_spec_str

    def compatible_with(self, other):
        if self.__eq__(other):
            return True

        if len(self.fields) != len(other.fields):
            return False

        index = 0
        for field in self.fields:
            other_field: PartitionField = other.fields[index]
            if (
                field.source_id != other_field.source_id
                or field.name != other_field.name
            ):
                index += 1
                # TODO: Add transform check
                return False
        return True

    def partition_type(self):
        struct_fields = []
        # TODO: Needs transform
        pass

    def escape(self, input_str):
        try:
            return parse.urlencode(input_str, encoding="utf-8")
        except TypeError as e:
            raise e

    def partition_to_path(self):
        # TODO: Needs transform
        pass

    def _generate_unpartitioned_spec(self):
        return PartitionSpec(
            schema=Schema(),
            spec_id=0,
            part_fields=[],
            last_assigned_field_id=PARTITION_DATA_ID_START - 1,
        )

    def unpartitioned(self) -> PartitionSpec:
        return self._generate_unpartitioned_spec()

    def check_compatibility(self, spec: PartitionSpec, schema: Schema):
        for field in spec.fields:
            source_type = schema.find_type(field.source_id)
            ValidationException().check(
                source_type != None,
                f"Cannot find source column for partition field: {field}",
            )
            ValidationException().check(
                source_type.is_primitive,
                f"Cannot partition by non-primitive source field: {source_type}",
            )
            # TODO: Add transform check

    def has_sequential_ids(self, spec: PartitionSpec):
        index = 0
        for field in spec.fields:
            if field.field_id != PARTITION_DATA_ID_START + index:
                return False
            index += 1
        return True


class AtomicInteger:
    # TODO: Move to utils
    def __init__(self, value=0):
        self._value = int(value)
        self._lock = threading.Lock()

    def inc(self, d=1):
        with self._lock:
            self._value += int(d)
            return self._value

    def dec(self, d=1):
        return self.inc(-d)

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = int(v)
            return self._value


class Builder(object):
    schema: Schema = None
    fields: List[PartitionField] = []
    partition_names = set()
    dedup_fields = dict()
    spec_id = 0
    last_assigned_field_id = AtomicInteger(value=PARTITION_DATA_ID_START - 1)

    def __init__(self, schema: Schema):
        self._schema = schema

    def builder_for(self, schema):
        return Builder(schema=schema)

    def next_field_id(self):
        return self.last_assigned_field_id.inc()

    def check_and_add_partition_name(self, name: str):
        # return check_and_add_partition_name(name, None)
        # TODO: needs more schema methods
        pass

    def check_and_add_partition_name(self, name: str, source_column_id: int):
        # TODO: needs more schema methods
        pass

    def check_for_redundant_partitions(self, field: PartitionField):
        # TODO: needs transforms
        pass

    def with_spec_id(self, new_spec_id: int) -> Builder:
        self.spec_id = new_spec_id
        return self

    def find_source_column(self, source_name: str) -> NestedField:
        # TODO: needs schema.find_field
        pass

    def identity(self, source_name: str, target_name: str) -> Builder:
        # TODO: needs check_for_redundant_partitions
        pass

    def identity(self, source_name: str) -> Builder:
        return self.identity(source_name, source_name)

    def year(self, source_name: str, target_name: str) -> Builder:
        # TODO: needs check_for_redundant_partitions
        pass

    def year(self, source_name: str) -> Builder:
        return self.year(source_name, source_name + "_year")

    def month(self, source_name: str, target_name: str) -> Builder:
        # TODO: needs check_for_redundant_partitions
        pass

    def month(self, source_name: str) -> Builder:
        return self.month(source_name, source_name + "_month")

    def day(self, source_name: str, target_name: str) -> Builder:
        # TODO: needs check_for_redundant_partitions
        pass

    def day(self, source_name: str) -> Builder:
        return self.day(source_name, source_name + "_day")

    def hour(self, source_name: str, target_name: str) -> Builder:
        # TODO: needs check_for_redundant_partitions
        pass

    def hour(self, source_name: str) -> Builder:
        return self.hour(source_name, source_name + "_hour")

    def bucket(self, source_name: str, num_buckets: int, target_name: str) -> Builder:
        # TODO: needs transforms
        pass

    def bucket(self, source_name: str, num_buckets: int) -> Builder:
        return self.bucket(source_name, num_buckets, source_name + "_bucket")

    def truncate(self, source_name: str, width: int, target_name: str) -> Builder:
        # TODO: needs check_and_add_partition_name and transforms
        pass

    def truncate(self, source_name: str, width: int) -> Builder:
        return self.truncate(source_name, width, source_name + "_trunc")

    def always_null(self, source_name: str, target_name: str) -> Builder:
        # TODO: needs check_and_add_partition_name and transforms
        pass

    def always_null(self, source_name: str) -> Builder:
        return self.always_null(source_name, source_name + "_null")

    def add(self, source_id: int, field_id: int):
        # TODO: needs transforms
        # TODO: Two more add methods are needed
        pass

    def build(self) -> PartitionSpec:
        spec = PartitionSpec(
            self.schema, self.spec_id, self.fields, self.last_assigned_field_id
        )
        PartitionSpec().check_compatibility(spec, self.schema)
        return spec
