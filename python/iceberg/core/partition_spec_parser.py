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
                 PartitionSpecParser.SOURCE_ID: field.source_id}
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
        if not isinstance(fields, (list, tuple)):
            raise RuntimeError("Cannot parse partition spec fields, not an array: %s" % fields)

        for element in fields:
            if not isinstance(element, dict):
                raise RuntimeError("Cannot parse partition field, not an object: %s" % element)

            builder.add(element.get(PartitionSpecParser.SOURCE_ID),
                        element.get(PartitionSpecParser.NAME),
                        element.get(PartitionSpecParser.TRANSFORM))

        return builder.build()

    @staticmethod
    def from_json_fields(schema, spec_id, json_obj):
        builder = PartitionSpec.builder_for(schema).with_spec_id(spec_id)

        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        if not isinstance(json_obj, list):
            raise RuntimeError("Cannot parse partition spec fields, not an array: %s" % json_obj)

        for item in json_obj:
            if not isinstance(item, dict):
                raise RuntimeError("Cannot parse partition field, not an object: %s" % json_obj)
            builder.add(item.get(PartitionSpecParser.SOURCE_ID),
                        item.get(PartitionSpecParser.NAME),
                        item.get(PartitionSpecParser.TRANSFORM))

        return builder.build()
