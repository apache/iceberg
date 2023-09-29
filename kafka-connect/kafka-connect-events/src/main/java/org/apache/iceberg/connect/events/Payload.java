/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.events;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.avro.AvroSchemaUtil;

public interface Element extends IndexedRecord, SchemaConstructable {
  // this is required by Iceberg's Avro deserializer to check for special metadata
  // fields, but we aren't using any
  String DUMMY_FIELD_ID = "-1";

  String FIELD_ID_PROP = AvroSchemaUtil.FIELD_ID_PROP;

  Schema UUID_SCHEMA =
      LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().fixed("uuid").size(16));
}
