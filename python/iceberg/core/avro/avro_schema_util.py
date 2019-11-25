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


class AvroSchemaUtil(object):
    FIELD_ID_PROP = "field-id"
    KEY_ID_PROP = "key-id"
    VALUE_ID_PROP = "value-id"
    ELEMENT_ID_PROP = "element-id"
    ADJUST_TO_UTC_PROP = "adjust-to_utc"

    # NULL = PrimitiveSchema(NULL)

    @staticmethod
    def convert(iceberg_schema=None, avro_schema=None, table_name=None, names=None,
                type_var=None, name=None):
        if iceberg_schema is not None and table_name is not None:
            return AvroSchemaUtil.convert(iceberg_schema=iceberg_schema,
                                          names={iceberg_schema.as_struct(): table_name})
        elif iceberg_schema is not None and names is not None:
            raise RuntimeError("Not yet implemented")
