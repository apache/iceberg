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

from iceberg.api.types import TypeID

hive_types = {
    TypeID.BOOLEAN: 'boolean',
    TypeID.INTEGER: 'int',
    TypeID.LONG: 'bigint',
    TypeID.FLOAT: 'float',
    TypeID.DOUBLE: 'double',
    TypeID.DATE: 'date',
    TypeID.TIME: 'string',
    TypeID.TIMESTAMP: 'timestamp',
    TypeID.STRING: 'string',
    TypeID.UUID: 'string',
    TypeID.FIXED: 'binary',
    TypeID.BINARY: "binary",
    TypeID.DECIMAL: None,
    TypeID.STRUCT: None,
    TypeID.LIST: None,
    TypeID.MAP: None
}
