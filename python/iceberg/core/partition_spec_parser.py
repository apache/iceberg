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

import json

from iceberg.api import PartitionSpec


class PartitionSpecParser(object):

    SPEC_ID = "spec-id"
    FIELDS = "fields"
    SOURCE_ID = "source-id"
    FIELD_ID = "field-id"
    TRANSFORM = "transform"
    NAME = "name"

    @staticmethod
    def to_json(spec, indent=None):
        return json.dumps(PartitionSpecParser.to_dict(spec), indent=indent)

    @staticmethod
    def to_dict(spec):
        return {PartitionSpecParser.SPEC_ID: spec.spec_id,
                PartitionSpecParser.FIELDS: PartitionSpecParser.to_json_fields(spec)}

    @staticmethod
    def to_json_fields(spec):
        return [{PartitionSpecParser.NAME: field.name,
                 PartitionSpecParser.TRANSFORM: str(field.transform),
                 PartitionSpecParser.SOURCE_ID: field.source_id,
                 PartitionSpecParser.FIELD_ID: field.field_id}
                for field in spec.fields]

    @staticmethod
    def from_json(schema, json_obj):
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        if not isinstance(json_obj, dict):
            raise RuntimeError("Cannot parse partition spec, not an object: %s" % json_obj)

        spec_id = json_obj.get(PartitionSpecParser.SPEC_ID)

        builder = PartitionSpec.builder_for(schema).with_spec_id(spec_id)
        fields = json_obj.get(PartitionSpecParser.FIELDS)

        return PartitionSpecParser.__build_from_json_fields(builder, fields)

    @staticmethod
    def from_json_fields(schema, spec_id, json_obj):
        builder = PartitionSpec.builder_for(schema).with_spec_id(spec_id)

        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        return PartitionSpecParser.__build_from_json_fields(builder, json_obj)

    @staticmethod
    def __build_from_json_fields(builder, json_fields):
        if not isinstance(json_fields, (list, tuple)):
            raise RuntimeError("Cannot parse partition spec fields, not an array: %s" % json_fields)

        field_id_count = 0
        for element in json_fields:
            if not isinstance(element, dict):
                raise RuntimeError("Cannot parse partition field, not an object: %s" % element)

            if element.get(PartitionSpecParser.FIELD_ID) is not None:
                builder.add(element.get(PartitionSpecParser.SOURCE_ID),
                            element.get(PartitionSpecParser.FIELD_ID),
                            element.get(PartitionSpecParser.NAME),
                            element.get(PartitionSpecParser.TRANSFORM))
                field_id_count = field_id_count + 1
            else:
                builder.add_without_field_id(element.get(PartitionSpecParser.SOURCE_ID),
                                             element.get(PartitionSpecParser.NAME),
                                             element.get(PartitionSpecParser.TRANSFORM))

        if field_id_count > 0 and field_id_count != len(json_fields):
            raise RuntimeError("Cannot parse spec with missing field IDs: %s missing of %s fields." %
                               (len(json_fields) - field_id_count, len(json_fields)))

        return builder.build()
